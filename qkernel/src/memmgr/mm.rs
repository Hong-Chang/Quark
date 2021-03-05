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

use alloc::sync::Arc;
use alloc::sync::Weak;
use spin::RwLock;
use spin::Mutex;
use core::ops::Deref;
use lazy_static::lazy_static;
use core::sync::atomic::AtomicU64;
use core::sync::atomic;
use alloc::string::String;
use alloc::string::ToString;
use alloc::slice;
use x86_64::structures::paging::PageTableFlags;
use alloc::vec::Vec;

use super::super::arch::x86_64::context::*;
use super::super::PAGE_MGR;
use super::super::KERNEL_PAGETABLE;
use super::super::qlib::common::*;
use super::super::qlib::linux_def::*;
use super::super::qlib::range::*;
use super::super::qlib::addr::*;
use super::super::qlib::stack::*;
use super::super::qlib::mem::seq::*;
use super::super::task::*;
use super::super::qlib::mem::stackvec::*;
use super::super::qlib::pagetable::*;
use super::super::qlib::limits::*;
use super::super::qlib::perf_tunning::*;
use super::super::fs::dirent::*;
use super::super::mm::*;
use super::super::qlib::mem::areaset::*;
use super::arch::*;
use super::vma::*;
use super::metadata::*;


#[derive(Clone)]
pub struct MemoryManagerInternal {
    pub inited: bool,
    pub pt: PageTables,
    pub vmas: AreaSet<VMA>,

    // brk is the mm's brk, which is manipulated using the brk(2) system call.
    // The brk is initially set up by the loader which maps an executable
    // binary into the mm.
    pub brkInfo: BrkInfointernal,

    // usageAS is vmas.Span(), cached to accelerate RLIMIT_AS checks.
    pub usageAS: u64,

    // layout is the memory layout.
    pub layout: MmapLayout,

    pub curRSS: u64,
    pub maxRSS: u64,

    pub sharedLoadsOffset: u64,
    pub auxv: Vec<AuxEntry>,
    pub argv: Range,
    pub envv: Range,
    pub executable: Option<Dirent>,

    // dumpability describes if and how this MemoryManager may be dumped to
    // userspace.
    //
    // dumpability is protected by metadataMu.
    pub dumpability: Dumpability,
    pub lock: Arc<Mutex<()>>,
}

impl Default for MemoryManagerInternal {
    fn default() -> Self {
        let vmas = AreaSet::New(0, MemoryDef::LOWER_TOP);

        return Self {
            inited: false,
            pt: PageTables::default(),
            vmas: vmas,
            brkInfo: BrkInfointernal::default(),
            usageAS: 0,
            layout: MmapLayout::default(),
            curRSS: 0,
            maxRSS: 0,
            sharedLoadsOffset: MemoryDef::SHARED_START,
            auxv: Vec::new(),
            argv: Range::default(),
            envv: Range::default(),
            executable: None,
            dumpability: NOT_DUMPABLE,
            lock: Arc::new(Mutex::new(())),
        }
    }
}

impl MemoryManagerInternal {
    pub fn Init() -> Self {
        let mut vmas = AreaSet::New(0, !0);
        let vma = VMA {
            mappable: None,
            offset: 0,
            fixed: true,
            realPerms: AccessType::ReadWrite(),
            effectivePerms: AccessType::ReadWrite(),
            maxPerms: AccessType::ReadWrite(),
            private: true,
            growsDown: false,
            kernel: true,
            hint: String::from("Kernel Space"),
            id: None,
        };

        let gap = vmas.FindGap(MemoryDef::PHY_LOWER_ADDR);
        vmas.Insert(&gap, &Range::New(MemoryDef::PHY_LOWER_ADDR, MemoryDef::PHY_UPPER_ADDR - MemoryDef::PHY_LOWER_ADDR), vma);

        let layout = MmapLayout {
            MinAddr: MemoryDef::VIR_MMAP_START,
            MaxAddr: MemoryDef::LOWER_TOP,
            BottomUpBase: MemoryDef::VIR_MMAP_START,
            TopDownBase: MemoryDef::LOWER_TOP,
            ..Default::default()
        };

        let pt = KERNEL_PAGETABLE.Fork(&*PAGE_MGR).unwrap();

        return Self {
            inited: true,
            pt: pt,
            vmas: vmas,
            brkInfo: BrkInfointernal::default(),
            usageAS: 0,
            layout: layout,
            curRSS: 0,
            maxRSS: 0,
            sharedLoadsOffset: MemoryDef::SHARED_START,
            auxv: Vec::new(),
            argv: Range::default(),
            envv: Range::default(),
            executable: None,
            dumpability: NOT_DUMPABLE,
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn MapStackAddr(&self) -> u64 {
        return self.layout.MapStackAddr();
    }

    //return: (phyaddr, iswriteable)
    pub fn VirtualToPhy(&self, vAddr: u64) -> Result<(u64, bool)> {
        return self.pt.read().VirtualToPhy(vAddr);
    }

    //Remove virtual memory to the phy mem mapping
    pub fn RemoveVMAsLocked(&mut self, mm: &MemoryManager, ar: &Range) -> Result<()> {
        let (mut vseg, vgap) = self.vmas.Find(ar.Start());
        if vgap.Ok() {
            vseg = vgap.NextSeg();
        }

        while vseg.Ok() && vseg.Range().Start() < ar.End() {
            vseg = self.vmas.Isolate(&vseg, &ar);
            let r = vseg.Range();
            let vma = vseg.Value();

            if vma.mappable.is_some() {
                let mappable = vma.mappable.clone().unwrap();
                mappable.RemoveMapping(mm, &r, vma.offset, vma.CanWriteMappableLocked())?;
            }

            self.usageAS -= r.Len();
            self.RemoveRssLock(&r);

            self.pt.write().MUnmap(r.Start(), r.Len())?;
            let vgap = self.vmas.Remove(&vseg);
            vseg = vgap.NextSeg();
        }

        return Ok(())
    }

    pub fn BrkSetup(&mut self, addr: u64) {
        self.brkInfo = BrkInfointernal {
            brkStart : addr,
            brkEnd: addr,
            brkMemEnd: addr,
        }
    }

    pub fn AddRssLock(&mut self, ar: &Range) {
        self.curRSS += ar.Len();
        if self.curRSS > self.maxRSS {
            self.maxRSS = self.curRSS;
        }
    }

    pub fn RemoveRssLock(&mut self, ar: &Range) {
        self.curRSS -= ar.Len();
    }
}

pub type UniqueID = u64;
lazy_static! {
    static ref UID: AtomicU64 = AtomicU64::new(1);
}

pub fn NewUID() -> u64 {
    return UID.fetch_add(1, atomic::Ordering::SeqCst);
}

#[derive(Clone)]
pub struct MemoryManagerWeak {
    pub uid: UniqueID,
    pub data: Weak<RwLock<MemoryManagerInternal>>,
}

impl MemoryManagerWeak {
    pub fn ID(&self) -> UniqueID {
        return self.uid;
    }

    pub fn Upgrade(&self) -> MemoryManager {
        return MemoryManager {
            uid: self.uid,
            data: self.data.upgrade().expect("MemoryManagerWeak upgrade fail"),
        }
    }
}

#[derive(Clone)]
pub struct MemoryManager {
    pub uid: UniqueID,
    pub data: Arc<RwLock<MemoryManagerInternal>>,
}

impl Deref for MemoryManager {
    type Target = Arc<RwLock<MemoryManagerInternal>>;

    fn deref(&self) -> &Arc<RwLock<MemoryManagerInternal>> {
        &self.data
    }
}

impl Drop for MemoryManager {
    fn drop(&mut self) {
        if self.read().inited == false {
            return;
        }

        if Arc::strong_count(&self.data) == 1 {
            //error!("MemoryManager::drop start to clear");
            self.Clear().expect("MemoryManager::Drop fail");
            //error!("MemoryManager::drop after to clear");
        }
    }
}

impl MemoryManager {
    pub fn Lock(&self) -> Arc<Mutex<()>> {
        return self.read().lock.clone();
    }

    pub fn Downgrade(&self) -> MemoryManagerWeak {
        return MemoryManagerWeak {
            uid: self.uid,
            data: Arc::downgrade(&self.data),
        }
    }

    pub fn Empty() -> Self {
        return Self {
            uid: NewUID(),
            data: Arc::new(RwLock::new(MemoryManagerInternal::default()))
        }
    }

    pub fn Init() -> Self {
        let internal = MemoryManagerInternal::Init();
        return Self {
            uid: NewUID(),
            data: Arc::new(RwLock::new(internal)),
        }
    }

    pub fn GenStatmSnapshot(&self, _task: &Task) -> Vec<u8> {
        let vss = self.read().curRSS;
        let rss = self.read().maxRSS;

        let res = format!("{} {} 0 0 0 0 0\n",
                          vss/MemoryDef::PAGE_SIZE, rss/MemoryDef::PAGE_SIZE);

        return res.as_bytes().to_vec();
    }

    pub const DEV_MINOR_BITS : usize = 20;
    pub const VSYSCALLEND: u64 = 0xffffffffff601000;
    pub const VSYSCALL_MAPS_ENTRY : &'static str  = "ffffffffff600000-ffffffffff601000 --xp 00000000 00:00 0                  [vsyscall]\n";

    pub fn GetSnapshot(&self, task: &Task, skipKernel: bool) -> String {
        let internal = self.read();
        let mut seg = internal.vmas.FirstSeg();
        let mut ret = "".to_string();
        loop {
            if seg.IsTail() {
                break;
            }

            let vma = seg.Value();
            if vma.kernel && skipKernel {
                seg = seg.NextSeg();
                continue;
            }

            let range = seg.Range();

            let private = if vma.private {
                "p"
            } else {
                "s"
            };

            let (dev, inodeId) = match &vma.id {
                None => (0, 0),
                Some(ref mapping) => {
                    (mapping.DeviceID(), mapping.InodeID())
                }
            };

            let devMajor = (dev >> Self::DEV_MINOR_BITS) as u32;
            let devMinor = (dev & ((1 <<Self::DEV_MINOR_BITS) - 1)) as u32;

            let mut s = if vma.hint.len() == 0 {
                vma.hint.to_string()
            } else {
                match &vma.id {
                    None => "".to_string(),
                    //todo: seems that mappedName doesn't work. Fix it
                    Some(ref id) => id.MappedName(task),
                }
            };

            let str = format!("{:08x}-{:08x} {}{} {:08x} {:02x}:{:02x} {} ",
                              range.Start(),
                              range.End(),
                              vma.realPerms.String(),
                              private,
                              vma.offset,
                              devMajor,
                              devMinor,
                              inodeId
            );

            if s.len() != 0 && str.len() < 73 {
                let pad = String::from_utf8(vec![b' '; 73 - str.len()]).unwrap();
                s = pad + &s;
            }

            ret += &str;
            ret += &s;
            ret += "\n";

            seg = seg.NextSeg();
        }

        ret += Self::VSYSCALL_MAPS_ENTRY;

        return ret;
        //return ret.as_bytes().to_vec();
    }

    pub fn GenMapsSnapshot(&self, task: &Task) -> Vec<u8> {
        let ret = self.GetSnapshot(task, true);

        return ret.as_bytes().to_vec();
    }

    pub fn SetExecutable(&self, dirent: &Dirent) {
        self.write().executable = Some(dirent.clone());
    }

    //remove all the user vmas, used for execve
    pub fn Clear(&self) -> Result<()> {
        // if we are clearing memory manager in current pagetable,
        // need to switch to kernel pagetable to avoid system crash
        let isCurrent = self.read().pt.IsActivePagetable();
        if isCurrent {
            super::super::KERNEL_PAGETABLE.SwitchTo();
        }

        let mut mm = self.write();
        let mut vseg = mm.vmas.FirstSeg();

        while vseg.Ok() {
            let r = vseg.Range();
            let vma = vseg.Value();

            if vma.kernel == true {
                vseg = vseg.NextSeg();
                continue;
            }

            if vma.mappable.is_some() {
                let mappable = vma.mappable.clone().unwrap();
                mappable.RemoveMapping(self, &r, vma.offset, vma.CanWriteMappableLocked())?;
            }

            mm.pt.write().MUnmap(r.Start(), r.Len())?;
            let vgap = mm.vmas.Remove(&vseg);
            vseg = vgap.NextSeg();
        }

        return Ok(())
    }

    pub fn SetMmapLayout(&self, minUserAddr: u64, maxUserAddr: u64, r: &LimitSet) -> Result<MmapLayout> {
        let layout = Context64::NewMmapLayout(minUserAddr, maxUserAddr, r)?;
        self.write().layout = layout;
        return Ok(layout)
    }

    pub fn GetRoot(&self) -> u64 {
        return self.read().pt.read().GetRoot()
    }

    pub fn GetVma(&self, addr: u64) -> Option<VMA> {
        let vseg = self.read().vmas.FindSeg(addr);
        if !vseg.Ok() {
            return None;
        }

        return Some(vseg.Value())
    }

    pub fn GetVmaAndRange(&self, addr: u64) -> Option<(VMA, Range)> {
        let vseg = self.read().vmas.FindSeg(addr);
        if !vseg.Ok() {
            return None;
        }

        return Some((vseg.Value(), vseg.Range()))
    }

    pub fn MapPage(&self, vaddr: Addr, phyAddr: Addr, flags: PageTableFlags) -> Result<bool> {
        let pt = self.read().pt.clone();
        return pt.write().MapPage(vaddr, phyAddr, flags, &*PAGE_MGR);
    }

    pub fn VirtualToPhy(&self, vAddr: u64) -> Result<(u64, bool)> {
        if vAddr == 0 {
            return Err(Error::SysError(SysErr::EFAULT))
        }

        let pt = self.read().pt.clone();
        return pt.read().VirtualToPhy(vAddr);
    }

    pub fn InstallPageWithAddr(&self, task: &Task, pageAddr: u64) -> Result<()> {
        let lock = self.Lock();
        let _l = lock.lock();

        let (vma, range) = match task.mm.GetVmaAndRange(pageAddr) {
            None => return Err(Error::SysError(SysErr::EFAULT)),
            Some(data) => data
        };

        return self.InstallPage(task, &vma, pageAddr, &range);
    }

    pub fn InstallPage(&self, task: &Task, vma: &VMA, pageAddr: u64, range: &Range) -> Result<()> {
        match task.VirtualToPhy(pageAddr) {
            Err(_) => (),
            Ok(_) => return Ok(())
        }

        match &vma.mappable {
            Some(ref mappable) => {
                let vmaOffset = pageAddr - range.Start();
                let fileOffset = vmaOffset + vma.offset; // offset in the file
                let phyAddr = mappable.MapFilePage(task, fileOffset)?;
                //error!("fault 2.1, vma.mappable.is_some() is {}, vaddr is {:x}, paddr is {:x}",
                 //      vma.mappable.is_some(), pageAddr, phyAddr);

                if vma.private {
                    self.MapPageRead(pageAddr, phyAddr);
                } else {
                    let writeable = vma.effectivePerms.Write();
                    if writeable {
                        self.MapPageWrite(pageAddr, phyAddr);
                    } else {
                        self.MapPageRead(pageAddr, phyAddr);
                    }
                }

                return Ok(())
            },
            None => {
                //let vmaOffset = pageAddr - range.Start();
                //let phyAddr = vmaOffset + vma.offset; // offset in the phyAddr

                let phyAddr = super::super::PAGE_MGR.AllocPage(false).unwrap();
                if vma.private {
                    self.MapPageRead(pageAddr, phyAddr);
                } else {
                    let writeable = vma.effectivePerms.Write();
                    if writeable {
                        self.MapPageWrite(pageAddr, phyAddr);
                    } else {
                        self.MapPageRead(pageAddr, phyAddr);
                    }
                }

                return Ok(())
            }
        }
    }

    // check whether the address range is legal.
    // 1. whether the range belong to user's space
    // 2. Whether the read/write permission meet requirement
    // 3. if need cow, fix the page.
    // 4. return max allowed len
    pub fn FixPermission(&self, task: &Task, vAddr: u64, len: u64, writeReq: bool, allowPartial: bool) -> Result<u64> {
        if core::u64::MAX - vAddr < len || vAddr == 0 {
            return Err(Error::SysError(SysErr::EFAULT))
        }

        let mut addr = Addr(vAddr).RoundDown()?.0;
        //error!("FixPermission vaddr {:x} addr {:x} len is {:x}", vAddr, addr, len);
        while addr <= vAddr + len - 1 {
            let (_, writable) = match self.VirtualToPhy(addr) {
                Err(Error::AddressNotMap(_)) => {
                    self.InstallPageWithAddr(task, addr)?;
                    self.VirtualToPhy(addr)?
                }
                Err(e) => {
                    return Err(e)
                }
                Ok(ret) => ret,
            };
            if writeReq && !writable {
                let vma = match self.GetVma(addr) {
                    None => {
                        if allowPartial && addr < vAddr {
                            return Err(Error::SysError(SysErr::EFAULT))
                        }

                        return Ok(addr - vAddr)
                    },
                    Some(vma) => vma.clone(),
                };

                if !vma.effectivePerms.Write() {
                    info!("CheckPermission: fail writeReq is {}, writeable is {}", writeReq, writable);
                    if allowPartial && addr < vAddr {
                        return Err(Error::SysError(SysErr::EFAULT))
                    }

                    return Ok(addr - vAddr)
                }

                self.CopyOnWrite(addr, &vma);
            }

            addr += MemoryDef::PAGE_SIZE;
        }

        return Ok(len);
    }

    pub fn CopyOnWriteLocked(&self, pageAddr: u64, vma: &VMA) {
        let (phyAddr, writable) = self.VirtualToPhy(pageAddr).expect(&format!("addr is {:x}", pageAddr));

        if writable {
            // another thread has cow, return
            return;
        }

        let refCount = super::super::PAGE_MGR.GetRef(phyAddr)
            .expect(&format!("CopyOnWrite PAGE_MGR GetRef addr {:x} fail", phyAddr));

        if refCount == 1 && vma.mappable.is_none() {
            //print!("CopyOnWriteLocked enable write ... pageaddr is {:x}", pageAddr);
            self.EnableWrite(pageAddr);
        } else {
            // Copy On Write
            let page = { super::super::PAGE_MGR.AllocPage(false).unwrap() };
            CopyPage(pageAddr, page);
            self.MapPageWrite(pageAddr, page);
        }

        unsafe { llvm_asm!("invlpg ($0)" :: "r" (pageAddr): "memory" ) };
    }

    pub fn CopyOnWrite(&self, pageAddr: u64, vma: &VMA) {
        let lock = self.Lock();
        let _l = lock.lock();

        PerfGoto(PerfType::PageFault);
        self.CopyOnWriteLocked(pageAddr, vma);
        PerfGofrom(PerfType::PageFault);
    }

    pub fn EnableWrite(&self, addr: u64) {
        let pt = self.read().pt.clone();
        let mut pt = pt.write();

        pt.SetPageFlags(Addr(addr), PageOpts::UserReadWrite().Val());
    }

    pub fn MapPageWrite(&self, vAddr: u64, pAddr: u64) {
        let pt = self.read().pt.clone();
        let mut pt = pt.write();
        pt.MapPage(Addr(vAddr), Addr(pAddr), PageOpts::UserReadWrite().Val(), &*PAGE_MGR).unwrap();
    }

    pub fn MapPageRead(&self, vAddr: u64, pAddr: u64) {
        let pt = self.read().pt.clone();
        let mut pt = pt.write();
        pt.MapPage(Addr(vAddr), Addr(pAddr), PageOpts::UserReadOnly().Val(), &*PAGE_MGR).unwrap();
    }

    pub fn PopulateVMA(&self, task: &Task, vmaSeg: &AreaSeg<VMA>, ar: &Range, precommit: bool, vdso: bool) -> Result<()> {
        let vma = vmaSeg.Value();
        let mut perms = vma.effectivePerms;

        //if it is filemapping and private, need cow.
        // if it is anon share, first marks it as writeable. When clone, mark it as readonly.
        if vma.private & vma.mappable.is_some() {
            perms.ClearWrite();
        }

        let pt = self.read().pt.clone();

        let segAr = vmaSeg.Range();
        match vma.mappable {
            None => {
                //anonymous mapping
                if !vdso {
                    self.write().AddRssLock(ar);
                } else {
                    //vdso: the phyaddress has been allocated and the address is vma.offset
                    pt.write().MapHost(task, ar.Start(), &IoVec::NewFromAddr(vma.offset, ar.Len() as usize), &perms, true)?;
                }
            }
            Some(mappable) => {
                //host file mapping
                // the map file mapfile cost is high. Only pre-commit it when the size < 4MB.
                // todo: improve that later

                if precommit && segAr.Len() < 0x200000 {
                    pt.MapFile(task, ar.Start(), &mappable, &Range::New(vma.offset + ar.Start() - segAr.Start(), ar.Len()), &perms, precommit)?;
                }
                self.write().AddRssLock(ar);
            }
        }

        return Ok(())
    }

    pub fn PopulateVMARemap(&self, task: &Task, vmaSeg: &AreaSeg<VMA>, ar: &Range, oldar: &Range, precommit: bool) -> Result<()> {
        let segAr = vmaSeg.Range();
        let vma = vmaSeg.Value();
        let mut perms = vma.effectivePerms;

        if vma.private & vma.mappable.is_some() { //if it is filemapping and private, need cow.
            perms.ClearWrite();
        }

        let pt = self.read().pt.clone();

        match vma.mappable {
            None => {
                pt.write().RemapAna(task, ar, oldar.Start(), &perms, true)?;
            }
            Some(mappable) => {
                //host file mapping
                pt.RemapFile(task, ar.Start(), &mappable, &Range::New(vma.offset + ar.Start() - segAr.Start(), ar.Len()), oldar, &perms, precommit)?;
                self.write().AddRssLock(ar);
            }
        }

        return Ok(())
    }

    pub fn Fork(&self) -> Result<Self> {
        let mm2 = Self::Empty();
        {
            let mm = self.read();

            let mut mmIntern2 = mm2.write();
            mmIntern2.inited = true;
            mmIntern2.brkInfo = mm.brkInfo;
            mmIntern2.usageAS = mm.usageAS;
            mmIntern2.layout = mm.layout;
            mmIntern2.curRSS = mm.curRSS;
            mmIntern2.maxRSS = mm.maxRSS;
            mmIntern2.sharedLoadsOffset = mm.sharedLoadsOffset;

            let range = mm.vmas.range;
            mmIntern2.vmas.Reset(range.Start(), range.Len());

            let mut srcvseg = mm.vmas.FirstSeg();
            let mut dstvgap = mmIntern2.vmas.FirstGap();
            let srcPt = mm.pt.clone();
            mmIntern2.pt = srcPt.Fork(&*PAGE_MGR)?;

            for aux in &mm.auxv {
                mmIntern2.auxv.push(*aux);
            }
            mmIntern2.argv = mm.argv;
            mmIntern2.envv = mm.envv;
            mmIntern2.executable = mm.executable.clone();

            let mut srcPt = srcPt.write();

            let dstPt = mmIntern2.pt.clone();
            let mut dstPt = dstPt.write();

            while srcvseg.Ok() {
                let vma = srcvseg.Value();
                let vmaAR = srcvseg.Range();

                if vma.mappable.is_some() {
                    let mappable = vma.mappable.clone().unwrap();

                    match mappable.AddMapping(&mm2, &vmaAR, vma.offset, vma.CanWriteMappableLocked()) {
                        Err(e) => {
                            let appRange = mmIntern2.ApplicationAddrRange();
                            mmIntern2.RemoveVMAsLocked(&mm2, &appRange)?;
                            return Err(e)
                        }
                        _ => (),
                    }
                }

                if vma.kernel == false {
                    //info!("vma kernel is {}, private is {}, hint is {}", vma.kernel, vma.private, vma.hint);
                    if vma.private {
                        //cow
                        srcPt.ForkRange(&mut dstPt, vmaAR.Start(), vmaAR.Len(), &*PAGE_MGR)?;
                    } else {
                        srcPt.CopyRange(&mut dstPt, vmaAR.Start(), vmaAR.Len(), &*PAGE_MGR)?;
                    }
                }

                dstvgap = mmIntern2.vmas.Insert(&dstvgap, &vmaAR, vma).NextGap();

                let tmp = srcvseg.NextSeg();
                srcvseg = tmp;
            }
        }

        return Ok(mm2);
    }

    //used by file truncate, remove the cow related mapping
    pub fn RetsetFileMapping(&self, task: &Task, ar: &Range, _invalidatePrivate: bool) {
        let mm = self.read();

        let vseg = mm.vmas.FindSeg(ar.Start());
        if !vseg.Ok() || vseg.Value().mappable.is_none() || vseg.Range().IsSupersetOf(ar) {
            panic!("MemoryManager::RetsetFileMapping invalid input")
        };

        let pt = mm.pt.clone();

        let vr = vseg.Range();

        let offset = ar.Start() - vr.Start();
        let vma = vseg.Value();
        let mappable = vma.mappable.unwrap();

        //reset the filerange
        pt.ResetFileMapping(task, ar.Start(), &mappable, &Range::New(vma.offset + offset, ar.Len()), &vma.realPerms).unwrap();
    }

    pub fn ID(&self) -> u64 {
        return self.uid;
    }

    fn GetBlocks(&self, start: u64, len: u64, bs: &mut StackVec<IoVec>, writeable: bool) -> Result<()> {
        let alignedStart = Addr(start).RoundDown()?.0;
        let aligntedEnd = Addr(start + len).RoundUp()?.0;

        let pages = ((aligntedEnd - alignedStart) / MemoryDef::PAGE_SIZE) as usize;
        let mut vec = StackVec::New(pages);

        let pt = self.read().pt.clone();

        if writeable {
            pt.write().GetAddresses(Addr(alignedStart), Addr(aligntedEnd), &mut vec)?;
        } else {
            pt.write().GetAddresses(Addr(alignedStart), Addr(aligntedEnd), &mut vec)?;
        }

        ToBlocks(bs, vec.Slice());

        let mut slice = bs.SliceMut();

        let startOff = start - alignedStart;
        slice[0].start += startOff;
        slice[0].len -= startOff as usize;

        let endOff = aligntedEnd - (start + len);
        slice[slice.len() - 1].len -= endOff as usize;

        return Ok(())
    }

    //get an array of readonly blocks, return entries count put in bs
    pub fn GetReadonlyBlocks(&self, start: u64, len: u64, bs: &mut StackVec<IoVec>) -> Result<()> {
        return self.GetBlocks(start, len, bs, false);
    }

    pub fn GetAddressesWithCOW(&self, start: u64, len: u64, bs: &mut StackVec<IoVec>) -> Result<()> {
        return self.GetBlocks(start, len, bs, true);
    }

    pub fn V2PIov(&self, task: &Task, start: u64, len: u64, output: &mut Vec<IoVec>, writable: bool) -> Result<()> {
        self.FixPermission(task, start, len, writable, false)?;

        let mut start = start;
        let end = start + len;

        while start < end {
            let next = if Addr(start).IsPageAligned() {
                start + MemoryDef::PAGE_SIZE
            } else {
                Addr(start).RoundUp().unwrap().0
            };

            match self.VirtualToPhy(start) {
                Err(e) => {
                    info!("convert to phyaddress fail, addr = {:x} e={:?}", start, e);
                    return Err(Error::SysError(SysErr::EFAULT))
                }
                Ok((pAddr, _)) => {
                    output.push(IoVec {
                        start: pAddr,
                        len: if end < next {
                            (end - start) as usize
                        } else {
                            (next - start) as usize
                        }, //iov.len,
                    });

                }
            }

            start = next;
        }

        return Ok(())
    }

    //Copy an Object to user memory, it is used only for the task_clone
    pub fn CopyOutObj<T: Sized + Copy>(&self, task: &Task, src: &T, dst: u64) -> Result<()> {
        let len = core::mem::size_of::<T>();

        let mut dsts = Vec::new();
        self.V2PIov(task, dst, len as u64, &mut dsts, true)?;
        let dsts = BlockSeq::NewFromSlice(&dsts);

        let srcAddr = src as * const _ as u64 as * const u8;
        let src = unsafe { slice::from_raw_parts(srcAddr, len) };

        dsts.CopyOut(src);
        return Ok(())
    }
}

pub fn ToBlocks(bs: &mut StackVec<IoVec>, arr: &[u64]) {
    let mut begin = arr[0];
    let mut expect = begin + MemoryDef::PAGE_SIZE;
    for i in 1..arr.len() {
        if arr[i] == expect {
            expect += MemoryDef::PAGE_SIZE;
        } else {
            bs.Push(IoVec::NewFromAddr(begin, (expect - begin) as usize));
            begin = arr[i];
            expect = begin + MemoryDef::PAGE_SIZE;
        }
    }

    bs.Push(IoVec::NewFromAddr(begin, (expect - begin) as usize));
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn TestToBlocks() {
        let mut bs = StackVec::New(100);

        let arr = [MemoryDef::PAGE_SIZE, 2 * MemoryDef::PAGE_SIZE, 3 * MemoryDef::PAGE_SIZE, 5 * MemoryDef::PAGE_SIZE];
        ToBlocks(&mut bs, &arr);

        let slice = bs.Slice();
        assert_eq!(slice[0], Block::NewFromAddr(MemoryDef::PAGE_SIZE, 3 * MemoryDef::PAGE_SIZE as usize));
        assert_eq!(slice[1], Block::NewFromAddr(5 * MemoryDef::PAGE_SIZE, MemoryDef::PAGE_SIZE as usize));
    }
}