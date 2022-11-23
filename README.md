# Revel

Revel is a Rust implementation of [leveldb](https://github.com/google/leveldb).

## Introduction

This is an experimental project to learn LevelDB and to practice Rust when learning Rust. I will figure out how a lsm-tree storage engine works through out this project. The final goal is to have a feature complete implementation of the C++ original using Rust programming language and it has a long way to go.

## Project Status
Now, this project is under development and the table list the progress.

|feature|description|branch|status|
------|--------|----|----|
|memtable|memtable of leveldb using skiplist|https://github.com/guimingyue/revel/tree/memtable|done|
|log reader and writer|log file reader and writer|https://github.com/guimingyue/revel/tree/log_appender|almost done|
|write batch|write batch interface for db|https://github.com/guimingyue/revel/tree/write_batch|WIP|
