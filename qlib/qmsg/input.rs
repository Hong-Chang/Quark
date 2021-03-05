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

#[derive(Debug, Copy, Clone)]
#[repr(align(128))]
pub enum HostInputMsg {
    FireTimer(FireTimer),
    FdNotify(FdNotify),
    IOBufWriteResp(IOBufWriteResp),
    PrintStrResp(PrintStrResp),
    WakeIOThreadResp(()),
}

//host call kernel
#[derive(Debug, Default, Copy, Clone)]
pub struct FireTimer {
    pub TimerId: u64,
    pub SeqNo: u64,
}

//host call kernel
#[derive(Debug, Default, Copy, Clone)]
pub struct FdNotify {
    pub fd: i32,
    pub mask: u32,
}


#[derive(Debug, Default, Copy, Clone)]
pub struct IOBufWriteResp {
    pub fd: i32,
    pub addr: u64,
    pub len: usize,
    pub ret: i64,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct PrintStrResp {
    pub addr: u64,
    pub len: usize,
}