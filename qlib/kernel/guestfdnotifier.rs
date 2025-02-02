// Copyright (c) 2021 Quark Container Authors / 2018 The gVisor Authors.
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

use crate::qlib::common::*;
use crate::qlib::linux_def::*;
use crate::qlib::fileinfo::*;
use crate::qlib::kernel::Kernel::HostSpace;
use crate::qlib::kernel::SHARESPACE;
use crate::qlib::kernel::GlobalIOMgr;
use crate::qlib::kernel::kernel::waiter::Queue;

pub fn SetWaitInfo(fd: i32, queue: Queue) {
    GlobalIOMgr().SetWaitInfo(fd, queue);
}

pub fn UpdateFD(fd: i32) -> Result<()> {
    return GlobalIOMgr().UpdateFD(fd);
}

pub fn NonBlockingPoll(fd: i32, mask: EventMask) -> EventMask {
    return HostSpace::NonBlockingPoll(fd, mask) as EventMask;
}

pub fn Notify(fd: i32, mask: EventMask) {
    GlobalIOMgr().Notify(fd, mask);
}

#[repr(C)]
#[repr(packed)]
#[derive(Default, Copy, Clone, Debug)]
pub struct EpollEvent {
    pub Event: u32,
    pub U64: u64,
}

impl IOMgr {
    pub fn VcpuWait(&self) -> u64 {
        let ret = HostSpace::VcpuWait();
        if ret < 0 {
            panic!("ProcessHostEpollWait fail with error {}", ret)
        };

        return ret as u64;
    }

    pub fn ProcessHostEpollWait(&self) {
        let ret = HostSpace::HostEpollWaitProcess();
        if ret < 0 {
            panic!("ProcessHostEpollWait fail with error {}", ret)
        };
    }

    pub fn ProcessEvents(&self, events: &[EpollEvent]) {
        for e in events {
            let fd = e.U64 as i32;
            let event = e.Event as EventMask;
            self.Notify(fd, event)
        }
    }

    fn waitfd(fd: i32, mask: EventMask) -> Result<()> {
        HostSpace::WaitFDAsync(fd, mask);

        return Ok(());
    }

    pub fn UpdateFD(&self, fd: i32) -> Result<()> {
        if SHARESPACE.config.read().UringEpollCtl {
            return self.UpdateFDAsync(fd);
        } else {
            return self.UpdateFDSync(fd);
        }
    }

    pub fn FdWaitInfo(&self, fd: i32) -> Option<FdWaitInfo> {
        let fdInfo = match self.GetByHost(fd) {
            Some(info) => info,
            None => return None
        };

        return Some(fdInfo.lock().waitInfo.clone());
    }

    pub fn UpdateFDAsync(&self, fd: i32) -> Result<()> {
        let fi = match self.FdWaitInfo(fd) {
            None => return Ok(()),
            Some(fi) => fi,
        };

        let epollfd = self.Epollfd();

        return fi.UpdateFDAsync(fd, epollfd);
    }

    pub fn UpdateFDSync(&self, fd: i32) -> Result<()> {
        let fi = match self.FdWaitInfo(fd) {
            None => return Ok(()),
            Some(fi) => fi,
        };

        return fi.UpdateFDSync(fd);
    }

    pub fn SetWaitInfo(&self, fd: i32, queue: Queue) {
        let waitinfo = FdWaitInfo::New(queue, 0);

        let fdInfo = match self.GetByHost(fd) {
            Some(info) => info,
            None => {
                panic!("UpdateWaitInfo panic...")
            }
        };

        fdInfo.UpdateWaitInfo(waitinfo);
        return;
    }

    pub fn Notify(&self, fd: i32, mask: EventMask) {
        let fi = match self.FdWaitInfo(fd) {
            None => return,
            Some(fi) => fi,
        };

        fi.Notify(mask);
    }
}