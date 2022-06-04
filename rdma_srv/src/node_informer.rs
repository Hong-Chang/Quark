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

use super::constants::*;
use crate::rdma_ctrlconn::*;
use crate::RDMA_CTLINFO;
use svc_client::quark_cm_service_client::QuarkCmServiceClient;
use svc_client::MaxResourceVersionMessage;
use svc_client::NodeMessage;
use tokio::time::*;
use tonic::Request;

pub mod svc_client {
    tonic::include_proto!("quarkcmsvc");
}

#[derive(Debug)]
pub struct NodeInformer {
    pub max_resource_version: i32,
}

impl NodeInformer {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut informer = Self {
            max_resource_version: 0,
        };
        let mut client = QuarkCmServiceClient::connect(GRPC_SERVER_ADDRESS).await?;

        let ref nodes_message = client.list_node(()).await?.into_inner().nodes;
        if nodes_message.len() > 0 {
            for node_message in nodes_message {
                informer.handle(node_message);
            }
        }

        Ok(informer)
    }
}

impl NodeInformer {
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            match self.run_watch().await {
                Ok(_) => {}
                Err(e) => {
                    println!("Node watch error: {:?}", e);
                }
            }

            // println!("NodeInformer sleeps 1 second for next watch session.");
            sleep(Duration::from_secs(1)).await;
        }        
    }

    async fn run_watch(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // todo Hong: shall share client. And be careful to handle when connection is broken and need reconnect. Watch pod, node, service shall using the same client
        let mut client = QuarkCmServiceClient::connect(GRPC_SERVER_ADDRESS).await?;
        let mut node_stream = client
            .watch_node(Request::new(MaxResourceVersionMessage {
                max_resource_version: self.max_resource_version,
                // max_resource_version: 0,
            }))
            .await?
            .into_inner();

        while let Some(node_message) = node_stream.message().await? {
            self.handle(&node_message);
        }
        Ok(())
    }

    fn handle(&mut self, node_message: &NodeMessage) {
        let ip = node_message.ip;
        let mut nodes_map = RDMA_CTLINFO.nodes.lock();
        if node_message.event_type == EVENT_TYPE_SET {
            let node = Node {
                hostname: node_message.hostname.clone(),
                ipAddr: ip,
                subnet: node_message.subnet,
                netmask: node_message.net_mask,
                timestamp: node_message.creation_timestamp,
                resource_version: node_message.resource_version,
            };
            nodes_map.insert(ip, node);
            if node_message.resource_version > self.max_resource_version {
                self.max_resource_version = node_message.resource_version;
            }
        } else if node_message.event_type == EVENT_TYPE_DELETE {
            if nodes_map.contains_key(&ip) {
                if nodes_map[&ip].resource_version < node_message.resource_version {
                    nodes_map.remove(&ip);
                }
            }
        }
        if node_message.resource_version > self.max_resource_version {
            self.max_resource_version = node_message.resource_version;
        }
        println!("Handled Node: {:?}", node_message);
        println!("Debug: nodes_map len:{} {:?}", nodes_map.len(), nodes_map);
    }
}
