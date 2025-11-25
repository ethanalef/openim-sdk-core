# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**openim-sdk-core** is a cross-platform Go instant messaging SDK for iOS, Android, Web (WASM), and PC platforms. It provides WebSocket-based long connection management, offline message synchronization, local database persistence (SQLite/IndexedDB), and event-driven callbacks.

**Tech Stack:** Go 1.23+, Protocol Buffers, SQLite (GORM) / IndexedDB, WebSocket, gzip compression

## Knowledge Base

Comprehensive architecture documentation is in the `knowledge/` folder:

### [knowledge/01_OVERVIEW.md](knowledge/01_OVERVIEW.md)
Complete architectural overview covering:
- Three-layer design pattern (public API, business logic, shared utilities)
- Core components: LoginMgr (central hub), LongConnMgr (WebSocket), MsgSyncer (message sync)
- Database layer architecture (SQLite for native, IndexedDB for WASM)
- Message synchronization engine with SEQ-based tracking
- Callback & event system with listener interfaces
- Cross-platform architecture patterns
- WASM implementation details

### [knowledge/02_ARCHITECTURE_DIAGRAM.md](knowledge/02_ARCHITECTURE_DIAGRAM.md)
Visual architecture diagrams showing:
- Layer architecture and component relationships
- Communication channels between goroutines
- Initialization & login sequence flow
- WebSocket message flow (encoding, compression, transmission)
- Message synchronization flow with SEQ tracking
- Callback flow from server push to app callbacks

### [knowledge/03_WEB_SOCKET_AND_CONNECTION_MANAGEMENT.md](knowledge/04_WEB_SOCKET_AND_CONNECTION_MANAGEMENT.md)
WebSocket implementation deep dive with detailed explanations for Java developers:
- Connection establishment flow and lifecycle
- Three-pump architecture: readPump, writePump, heartbeat goroutines
- Goroutines vs threads comparison
- Channels vs BlockingQueue patterns
- Exponential backoff reconnection strategy
- Message encoding/compression pipeline (Protobuf → GOB → gzip)
- Async request-response tracking mechanism
- Ping-pong heartbeat and timeout handling
- Thread safety with mutexes and atomic operations

## Architecture Quick Reference

### Three-Layer Design
- **open_im_sdk/** - Public API layer: init_login.go, caller.go (reflection-based RPC), callbacks
- **internal/** - Business logic: interaction/ (WebSocket, sync), conversation_msg/, group/, user/, relation/, third/
- **pkg/** - Shared utilities: db/ (database), cache/, api/ (REST client), syncer/, utils/

### Core Components
- **LoginMgr** (open_im_sdk/userRelated.go) - Central orchestrator managing all domain managers, infrastructure, listeners, and channels
- **LongConnMgr** (internal/interaction/long_conn_mgr.go) - WebSocket connection manager with 3-pump goroutine pattern
- **MsgSyncer** (internal/interaction/msg_sync.go) - Message synchronization using SEQ-based incremental sync
- **DataBase** (pkg/db/db_interface/) - Interface-based design: SQLite (native) or IndexedDB (WASM)

### Key Architectural Patterns
- **Goroutines & Channels** - Lightweight concurrency with channel-based communication (actor pattern)
- **Context-driven Lifecycle** - All operations use context.Context for cancellation and timeout management
- **Interface-based Design** - Enables cross-platform implementations (database, network, callbacks)
- **SEQ-based Synchronization** - Server-assigned sequence numbers for reliable message ordering and gap detection

## Development Notes

### WebSocket Architecture
The WebSocket connection uses 3 concurrent goroutines (pump pattern): readPump reads and deserializes messages, writePump sends queued messages, and heartbeat sends ping frames every 24 seconds. Automatic reconnection with exponential backoff (1s → 2s → 4s → 8s → 16s). Message pipeline: Protobuf → GOB encode → gzip compress → binary WebSocket frame.

### Message Synchronization
Each conversation tracks a SEQ number (server-assigned). On reconnection/push/foreground, the SDK requests messages with seq > lastSyncedSeq (stored in DB and memory). Batch pulls up to 100 messages per request. Detects and fills message gaps automatically.

### Database Layer
Per-user database at {dataDir}/{userID}/im_{platformID}.db with tables for messages, conversations, friends, groups. Interface-based design allows SQLite (GORM) for native platforms and IndexedDB for WASM with identical schema.

### Communication Channels
Four main channels coordinate goroutines: conversationCh (conversation updates), cmdWsCh (WebSocket commands), msgSyncerCh (sync events), loginMgrCh (login state changes like token expiration).

### Cross-Platform Development
When adding features, update both native implementations (pkg/db/ SQLite, ws_default.go) and WASM implementations (wasm/indexdb/ IndexedDB, ws_js.go). Use build tags: //go:build js && wasm for WASM-specific code, //go:build !js || !wasm for native.

### Error Handling
Token errors (TokenExpiredError, TokenInvalidError, TokenKickedError) trigger SDK callbacks and stop reconnection. Other errors trigger retry with exponential backoff. Error types in pkg/sdkerrs/: ErrArgs, ErrSdkInternal, ErrResourceLoad, ErrCtxDeadline.
