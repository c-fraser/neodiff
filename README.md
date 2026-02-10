# neodiff

[![Test](https://github.com/c-fraser/neodiff/workflows/test/badge.svg)](https://github.com/c-fraser/neodiff/actions)
[![Release](https://img.shields.io/github/v/release/c-fraser/neodiff?logo=github&sort=semver)](https://github.com/c-fraser/neodiff/releases)
[![Crates.io](https://img.shields.io/crates/v/neodiff.svg)](https://crates.io/crates/neodiff)
[![Documentation](https://docs.rs/neodiff/badge.svg)](https://docs.rs/neodiff)
[![Apache License 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

A tool to compare and display differences between [Neo4j](http://neo4j.com/) graphs.

<video autoplay loop muted playsinline width="100%">                                                                                                                               
<source src="demo/demo.webm" type="video/webm">                                                                                                                                  
</video>  

## Install

### Rust

```shell
cargo install neodiff
```

> Requires [Rust](https://rust-lang.org/) *1.85+*.

### Releases

Download a `neodiff` distribution from a [release](https://github.com/c-fraser/neodiff/releases).

## Usage

```shell
neodiff --source <URI> --target <URI>
```

> The `URI` format is `bolt://[user]:[password]@[host]:[port]/[database]`.

The *source* and *target* Neo4j deployments must have the [APOC](https://neo4j.com/docs/apoc/current/) plugin
installed.

> [apoc.hashing.fingerprint](https://neo4j.com/docs/apoc/current/overview/apoc.hashing/apoc.hashing.fingerprinting/) is
> used to identify entities lacking a node key or
> uniqueness [constraint](https://neo4j.com/docs/cypher-manual/current/constraints/),
> and [apoc.map.fromPairs](https://neo4j.com/docs/apoc/current/overview/apoc.map/apoc.map.fromPairs/) is used to
> filter each entity's properties based on the key exclusions.

### Demo

Refer to the *Docker Compose* [demo](https://github.com/c-fraser/neodiff/blob/main/demo/run.sh).

## License

    Copyright 2025 c-fraser
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        https://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
