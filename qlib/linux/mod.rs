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

pub mod time;
pub mod signal;
pub mod limits;
pub mod futex;
pub mod sem;
pub mod ipc;
pub mod shm;
pub mod inotify;
pub mod netdevice;
pub mod socket;
pub mod rusage;
pub mod fcntl;
pub mod membarrier;

pub type TimeID = i32;