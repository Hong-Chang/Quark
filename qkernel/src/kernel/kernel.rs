// Copyright (c) 2021 QuarkSoft LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use spin::Mutex;
use spin::RwLock;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;
use alloc::string::String;
use alloc::string::ToString;
use core::ops::Deref;
use lazy_static::lazy_static;

use super::super::uid::NewUID;
use super::super::qlib::common::*;
use super::super::qlib::linux_def::*;
use super::super::qlib::stack::*;
use super::super::task::*;
//use super::super::qlib::context::Context;
use super::super::qlib::cpuid::*;
use super::super::qlib::auth::userns::*;
use super::super::qlib::auth::*;
use super::super::qlib::limits::*;
use super::super::qlib::path::*;
use super::super::loader::loader::*;
use super::super::SignalDef::*;
use super::super::threadmgr::pid_namespace::*;
use super::super::threadmgr::thread::*;
use super::super::threadmgr::threads::*;
use super::super::threadmgr::thread_group::*;
use super::super::fs::mount::*;
use super::super::fs::dirent::*;
use super::ipc_namespace::*;
use super::uts_namespace::*;
use super::fd_table::*;
use super::signal_handler::*;
use super::timer::timer::*;
use super::timer::timekeeper::*;
use super::timer::*;
use super::super::threadmgr::task_start::*;
use super::cpuset::*;
use super::time::*;
use super::platform::*;

lazy_static! {
    pub static ref KERNEL: Mutex<Option<Kernel>> = Mutex::new(None);
}

#[inline]
pub fn GetKernel() -> Kernel {
    return KERNEL.lock().clone().unwrap();
}

#[derive(Default)]
pub struct StaticInfo {
    // ApplicationCores is the number of logical CPUs visible to sandboxed
    // applications. The set of logical CPU IDs is [0, ApplicationCores); thus
    // ApplicationCores is analogous to Linux's nr_cpu_ids, the index of the
    // most significant bit in cpu_possible_mask + 1.
    pub ApplicationCores: u32,
    pub useHostCores: bool,

    // cpu is the fake cpu number returned by getcpu(2). cpu is ignored
    // entirely if Kernel.useHostCores is true.
    pub cpu: i32,
}

#[derive(Default)]
pub struct KernelInternal {
    // extMu serializes external changes to the Kernel with calls to
    // Kernel.SaveTo. (Kernel.SaveTo requires that the state of the Kernel
    // remains frozen for the duration of the call; it requires that the Kernel
    // is paused as a precondition, which ensures that none of the tasks
    // running within the Kernel can affect its state, but extMu is required to
    // ensure that concurrent users of the Kernel *outside* the Kernel's
    // control cannot affect its state by calling e.g.
    // Kernel.SendExternalSignal.)
    pub extMu: Mutex<()>,

    // See InitKernelArgs for the meaning of these fields.
    pub featureSet: Arc<Mutex<FeatureSet>>,
    pub tasks: TaskSet,
    pub rootUserNamespace: UserNameSpace,
    pub rootUTSNamespace: UTSNamespace,
    pub rootIPCNamespace: IPCNamespace,
    pub applicationCores: usize,
    //pub useHostCores: bool,

    // mounts holds the state of the virtual filesystem.
    pub mounts: RwLock<Option<MountNs>>,

    // globalInit is the thread group whose leader has ID 1 in the root PID
    // namespace. globalInit is stored separately so that it is accessible even
    // after all tasks in the thread group have exited, such that ID 1 is no
    // longer mapped.
    //
    // globalInit is mutable until it is assigned by the first successful call
    // to CreateProcess, and is protected by extMu.
    pub globalInit: Mutex<Option<ThreadGroup>>,

    // cpuClock is incremented every linux.ClockTick. cpuClock is used to
    // measure task CPU usage, since sampling monotonicClock twice on every
    // syscall turns out to be unreasonably expensive. This is similar to how
    // Linux does task CPU accounting on x86 (CONFIG_IRQ_TIME_ACCOUNTING),
    // although Linux also uses scheduler timing information to improve
    // resolution (kernel/sched/cputime.c:cputime_adjust()), which we can't do
    // since "preeemptive" scheduling is managed by the Go runtime, which
    // doesn't provide this information.
    //
    // cpuClock is mutable, and is accessed using atomic memory operations.
    pub cpuClock: AtomicU64,

    pub staticInfo: Mutex<StaticInfo>,

    pub cpuClockTicker: Option<Timer>,

    pub startTime: Time,

    pub platform: DefaultPlatform,
}

impl KernelInternal {
    pub fn newThreadGroup(&self, ns: &PIDNamespace,
                          sh: &SignalHandlers,
                          terminalSignal: Signal,
                          limit: &LimitSet) -> ThreadGroup {
        let internal = ThreadGroupInternal {
            pidns: ns.clone(),
            signalHandlers: sh.clone(),
            terminationSignal: terminalSignal,
            limits: limit.clone(),
            ..Default::default()
        };

        return ThreadGroup {
            uid: NewUID(),
            data: Arc::new(Mutex::new(internal))
        }
    }
}

#[derive(Clone, Default)]
pub struct Kernel(Arc<KernelInternal>);

impl Deref for Kernel {
    type Target = Arc<KernelInternal>;

    fn deref(&self) -> &Arc<KernelInternal> {
        &self.0
    }
}

impl Kernel {
    pub fn Init(args: InitKernalArgs) -> Self {
        let internal = KernelInternal {
            extMu: Mutex::new(()),
            featureSet: args.FeatureSet,
            tasks: TaskSet::New(),
            rootUserNamespace: args.RootUserNamespace,
            rootUTSNamespace: args.RootUTSNamespace,
            rootIPCNamespace: args.RootIPCNamespace,
            applicationCores: args.ApplicationCores as usize - 1,
            mounts: RwLock::new(None),
            globalInit: Mutex::new(None),
            cpuClock: AtomicU64::new(0),
            staticInfo: Mutex::new(StaticInfo {
                ApplicationCores: args.ApplicationCores,
                useHostCores: false,
                cpu: 0,
            }),
            cpuClockTicker: None,
            startTime: Task::RealTimeNow(),
            platform: DefaultPlatform::default(),
        };

        return Self(Arc::new(internal))
    }

    pub fn ApplicationCores(&self) -> u32 {
        return self.staticInfo.lock().ApplicationCores;
    }

    // TaskSet returns the TaskSet.
    pub fn TaskSet(&self) -> TaskSet {
        return self.tasks.clone();
    }

    pub fn TimeKeeper(&self) -> TimeKeeper {
        return TIME_KEEPER.clone()
    }

    pub fn RootDir(&self) -> Dirent {
        let mns = self.mounts.read().clone().unwrap();
        return mns.Root();
    }

    pub fn RootPIDNamespace(&self) -> PIDNamespace {
        return self.tasks.Root();
    }

    pub fn RootUserNamespace(&self) -> UserNameSpace {
        return self.rootUserNamespace.clone();
    }

    pub fn RootUTSNamesapce(&self) -> UTSNamespace {
        return self.rootUTSNamespace.clone();
    }

    pub fn RootIPCNamespace(&self) -> IPCNamespace {
        return self.rootIPCNamespace.clone();
    }

    pub fn CreateProcess(&self, args: &mut CreateProcessArgs) -> Result<(ThreadGroup, ThreadID)> {
        self.extMu.lock();

        let root = self.tasks.Root();
        let tg = self.newThreadGroup(&root, &SignalHandlers::default(), Signal(Signal::SIGCHLD), &args.Limits);
        tg.lock().liveThreads.Add(1);

        if args.Filename.as_str() == "" {
            if args.Argv.len() == 0 {
                return Err(Error::Common("no filename or command provided".to_string()))
            }

            if !IsAbs(&args.Argv[0]) {
                return Err(Error::Common(format!("'{}' is not an absolute path", args.Argv[0]).to_string()))
            }

            args.Filename = args.Argv[0].to_string();
        }

        let task = Task::Current();
        let mns = self.mounts.read().clone().unwrap();
        let root = mns.Root();
        task.fsContext.SetRootDirectory(&root);
        task.mountNS = mns.clone();

        let mut remainTraversals = MAX_SYMLINK_TRAVERSALS;
        let cwdDir = mns.FindInode(task, &root, None, &args.WorkingDirectory, &mut remainTraversals).expect("can't get cwd dirent");
        task.fsContext.SetWorkDirectory(&cwdDir);

        let config = TaskConfig {
            TaskId: task.taskId,
            Kernel: self.clone(),
            Parent: None,
            InheritParent: None,
            ThreadGroup: tg.clone(),
            SignalMask: SignalSet(0),
            MemoryMgr: task.mm.clone(),
            FSContext: task.fsContext.clone(),
            // FSContext::New(&root, &wd, args.Umask),
            Fdtbl: task.fdTbl.clone(),
            Credentials: args.Credentials.clone(),
            Niceness: 0,
            NetworkNamespaced: false,
            AllowedCPUMask: CPUSet::NewFullCPUSet(self.applicationCores),
            UTSNamespace: args.UTSNamespace.clone(),
            IPCNamespace: args.IPCNamespace.clone(),
            Blocker: task.blocker.clone(),
            ContainerID: args.ContainerID.to_string(),
        };

        let ts = self.tasks.clone();
        ts.NewTask(&config, true, self)?;

        let root = ts.Root();
        let tgid = root.IDOfThreadGroup(&tg);

        let isNone = self.globalInit.lock().is_none();
        if isNone {
            *self.globalInit.lock() = Some(tg.clone());
        }

        return Ok((tg, tgid))
    }

    pub fn GlobalInit(&self) -> ThreadGroup {
        self.extMu.lock();
        return self.globalInit.lock().clone().unwrap();
    }

    pub fn CPUClockNow(&self) -> u64 {
        return self.cpuClock.load(Ordering::SeqCst)
    }

    pub fn LoadProcess(&self, fileName: &str, envs: &Vec<String>, args: &mut Vec<String>) -> Result<(u64, u64, u64)>  {
        if self.globalInit.lock().is_none() {
            panic!("kernel contains no tasks");
        }

        let tasks = self.tasks.clone();
        let _r = tasks.ReadLock();
        /*let root = tasks.read().root.clone().unwrap();

        assert!(root.lock().tids.len() == 1, "Kernel::Start tids count is more than 1");

        let mut threads: Vec<Thread> = Vec::new();
        for (t, _tid) in &root.lock().tids {
            threads.push(t.clone());
        };

        assert!(threads.len() == 1, "ThreadGroup start has multiple threads");*/


        let task = Task::Current();
        return Load(task, fileName, args, envs, &Vec::new());

        //return Thread::Start(fileName, envs, args);
    }

    // Pause requests that all tasks in k temporarily stop executing, and blocks
    // until all tasks in k have stopped. Multiple calls to Pause nest and require
    // an equal number of calls to Unpause to resume execution.
    pub fn Pause(&self) {
        self.extMu.lock();
        self.tasks.BeginExternalStop();
    }

    // Unpause ends the effect of a previous call to Pause. If Unpause is called
    // without a matching preceding call to Pause, Unpause may panic.
    pub fn Unpause(&self) {
        self.extMu.lock();
        self.tasks.EndExternalStop();
    }

    pub fn SignalAll(&self, info: &SignalInfo) -> Result<()> {
        self.extMu.lock();
        let tasks = self.tasks.read();

        let root = tasks.root.as_ref().unwrap().clone();
        let mut lastErr = Ok(());

        for (tg, _) in &root.lock().tgids {
            let lock = tg.lock().signalLock.clone();
            let _l = lock.lock();
            let leader = tg.lock().leader.Upgrade();
            match leader.unwrap().sendSignalLocked(info, true) {
                Err(e) => lastErr = Err(e),
                Ok(()) => (())
            }
        }

        return lastErr
    }

    pub fn SendContainerSignal(&self, cid: &str, info: &SignalInfo) -> Result<()> {
        self.extMu.lock();
        let _r = self.tasks.ReadLock();
        let tasks = self.tasks.read();

        let root = tasks.root.as_ref().unwrap().clone();
        let mut lastErr = Ok(());

        let tgs : Vec<_> = root.lock().tgids.keys().cloned().collect();
        for tg in &tgs {
            let lock = tg.lock().signalLock.clone();
            let _l = lock.lock();
            let leader = tg.lock().leader.Upgrade().unwrap();
            if &leader.ContainerID() != cid {
                continue;
            }

            match leader.sendSignalLocked(info, true) {
                Err(e) => lastErr = Err(e),
                Ok(()) => (())
            }
        }

        return lastErr
    }
}

pub trait Context {
    fn CtxKernel(&self) -> Kernel;

    fn CtxPIDNamespace(&self) -> PIDNamespace;

    fn CtxUTSNamespace(&self) -> UTSNamespace;

    fn CtxIPCNamespace(&self) -> IPCNamespace;

    fn CtxCredentials(&self) -> Credentials;

    fn CtxRoot(&self) -> Dirent;
}

pub struct CreateProcessContext<'a> {
    k: Kernel,
    args: &'a CreateProcessArgs,
}

impl<'a> Context for CreateProcessContext<'a> {
    fn CtxKernel(&self) -> Kernel {
        return self.k.clone()
    }

    fn CtxPIDNamespace(&self) -> PIDNamespace {
        self.k.tasks.Root()
    }

    fn CtxUTSNamespace(&self) -> UTSNamespace {
        return self.args.UTSNamespace.clone()
    }

    fn CtxIPCNamespace(&self) -> IPCNamespace {
        return self.args.IPCNamespace.clone();
    }

    fn CtxCredentials(&self) -> Credentials {
        return self.args.Credentials.clone();
    }

    fn CtxRoot(&self) -> Dirent {
        if let Some(root) = self.args.Root.clone() {
            return root
        }

        let root = self.k.mounts.read().as_ref().unwrap().root.clone();

        return root
    }
}

#[derive(Default)]
pub struct InitKernalArgs {
    // FeatureSet is the emulated CPU feature set.
    pub FeatureSet: Arc<Mutex<FeatureSet>>,

    // RootUserNamespace is the root user namespace.
    pub RootUserNamespace: UserNameSpace,

    // ApplicationCores is the number of logical CPUs visible to sandboxed
    // applications. The set of logical CPU IDs is [0, ApplicationCores); thus
    // ApplicationCores is analogous to Linux's nr_cpu_ids, the index of the
    // most significant bit in cpu_possible_mask + 1.
    pub ApplicationCores: u32,

    // ExtraAuxv contains additional auxiliary vector entries that are added to
    // each process by the ELF loader.
    pub ExtraAuxv: Vec<AuxEntry>,

    // RootUTSNamespace is the root UTS namespace.
    pub RootUTSNamespace: UTSNamespace,

    // RootIPCNamespace is the root IPC namespace.
    pub RootIPCNamespace: IPCNamespace,
}

#[derive(Default)]
pub struct CreateProcessArgs {
    // Filename is the filename to load.
    //
    // If this is provided as "", then the file will be guessed via Argv[0].
    pub Filename: String,

    // Argvv is a list of arguments.
    pub Argv: Vec<String>,

    // Envv is a list of environment variables.
    pub Envv: Vec<String>,

    // WorkingDirectory is the initial working directory.
    //
    // This defaults to the root if empty.
    pub WorkingDirectory: String,

    // Credentials is the initial credentials.
    pub Credentials: Credentials,

    // FDMap is the initial set of file descriptors. If CreateProcess succeeds,
    // it takes a reference on FDMap.
    pub FdTable: FDTable,

    // Umask is the initial umask.
    pub Umask: u32,

    // Limits is the initial resource limits.
    pub Limits: LimitSet,

    // MaxSymlinkTraversals is the maximum number of symlinks to follow
    // during resolution.
    pub MaxSymlinkTraversals: u32,

    // UTSNamespace is the initial UTS namespace.
    pub UTSNamespace: UTSNamespace,

    // IPCNamespace is the initial IPC namespace.
    pub IPCNamespace: IPCNamespace,

    // Root optionally contains the dirent that serves as the root for the
    // process. If nil, the mount namespace's root is used as the process'
    // root.
    //
    // Anyone setting Root must donate a reference (i.e. increment it) to
    // keep it alive until it is decremented by CreateProcess.
    pub Root: Option<Dirent>,

    // ContainerID is the container that the process belongs to.
    pub ContainerID: String,

    pub Stdiofds: [i32; 3],
    pub Terminal: bool,
}