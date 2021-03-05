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

use std::os::unix::io::AsRawFd;

use super::super::qlib::common::*;
use super::pty::*;
use super::unix_socket::*;

pub fn NewWithSocket(socketPath: &str) -> Result<Master> {
    let master = NewMaster()?;

    let client = UnixSocket::NewClient(socketPath)?;
    client.SendFd(master.as_raw_fd())?;

    return Ok(master)
}
