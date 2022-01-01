use alloc::sync::Arc;

use super::super::super::qlib::common::*;
use super::super::super::qlib::linux_def::*;
use super::super::super::qlib::qmsg::qcall::*;
use super::super::super::qlib::socket_buf::*;
use super::super::super::task::*;
use super::super::super::Kernel::HostSpace;
//use super::super::super::kernel::waiter::*;

pub struct RDMA {}

impl RDMA {
    pub fn Accept(fd: i32, acceptQueue: &AcceptQueue) -> Result<AcceptItem> {
        let (trigger, ai) = acceptQueue.lock().DeqSocket();
        if trigger {
            HostSpace::RDMANotify(fd, RDMANotifyType::Accept);
        }

        return ai
    }

    pub fn Read(task: &Task, fd: i32, buf: Arc<SocketBuff>, dsts: &mut [IoVec]) -> Result<i64> {
        let (trigger, cnt) = buf.Readv(task, dsts)?;

        if trigger {
            HostSpace::RDMANotify(fd, RDMANotifyType::Read);
        }

        return Ok(cnt as i64)
    }

    //todo: put ops: &SocketOperations in the write request to make the socket won't be closed before write is finished
    pub fn Write(task: &Task, fd: i32, buf: Arc<SocketBuff>, srcs: &[IoVec]/*, ops: &SocketOperations*/) -> Result<i64> {
        let (count, writeBuf) = buf.Writev(task, srcs)?;

        if writeBuf.is_some() {
            HostSpace::RDMANotify(fd, RDMANotifyType::Write);
        }

        return Ok(count as i64)
    }
}