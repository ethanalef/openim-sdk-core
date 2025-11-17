# OpenIM SDK Core - Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Core Architecture](#core-architecture)
3. [Key Components](#key-components)
4. [Initialization & Login Flow](#initialization--login-flow)
5. [WebSocket & Connection Management](#websocket--connection-management)
6. [Message Synchronization](#message-synchronization)
7. [Database & Cache Layer](#database--cache-layer)
8. [Callback & Event System](#callback--event-system)
9. [Cross-Platform Architecture](#cross-platform-architecture)
10. [WASM Implementation](#wasm-implementation)

---

## Overview

**openim-sdk-core** is a cross-platform instant messaging SDK written in Go. It's designed to work across iOS, Android, Web (WASM), and PC platforms by providing:

- Core IM functionality (messaging, groups, friends, users)
- WebSocket-based long connection management
- Offline message synchronization
- Local database persistence (SQLite via GORM)
- Event-driven callback system
- Binary protocol encoding/decoding using Protocol Buffers

**Technology Stack:**
- Go 1.23+
- Protocol Buffers for message serialization
- SQLite (via GORM) for local storage
- WebSocket for network communication (gorilla/websocket, coder/websocket)
- gzip compression for message payload optimization

---

## Core Architecture

### Three-Layer Design Pattern

The SDK follows a clear three-layer architecture:

```
┌─────────────────────────────────────┐
│    open_im_sdk/                     │  <- Public SDK Interface
│    (Entry points for all operations)│     (init_login.go, apicb.go, caller.go)
└─────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────┐
│    internal/                        │  <- Business Logic
│  ├─ conversation_msg/               │     Manages conversations, messages
│  ├─ interaction/                    │     WebSocket, message sync
│  ├─ group/                          │     Group operations
│  ├─ user/                           │     User information & status
│  ├─ relation/                       │     Friend/blacklist management
│  └─ third/                          │     File uploads, third-party APIs
└─────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────┐
│    pkg/                             │  <- Shared Utilities
│  ├─ db/                             │     SQLite database operations
│  ├─ cache/                          │     In-memory caching (sync.Map)
│  ├─ api/                            │     REST API calls to server
│  ├─ network/                        │     Network utilities
│  ├─ constant/                       │     Constants & enums
│  ├─ sdkerrs/                        │     Error types
│  ├─ utils/                          │     Helper utilities
│  └─ syncer/                         │     Message synchronization logic
└─────────────────────────────────────┘
```

### LoginMgr: The Central Hub

`LoginMgr` (in `open_im_sdk/userRelated.go`) is the core orchestrator that manages all SDK operations:

```go
type LoginMgr struct {
    // Domain managers
    relation     *relation.Relation          // Friends, blacklist
    group        *group.Group                // Group operations
    conversation *conv.Conversation          // Conversations & messages
    user         *user.User                  // User profiles & status
    file         *file.File                  // File operations
    
    // Infrastructure
    db           db_interface.DataBase       // SQLite database
    longConnMgr  *interaction.LongConnMgr   // WebSocket connection
    msgSyncer    *interaction.MsgSyncer      // Message sync orchestrator
    third        *third.Third                // Third-party APIs
    
    // Event listeners
    groupListener        OnGroupListener
    friendshipListener   OnFriendshipListener
    conversationListener OnConversationListener
    advancedMsgListener  OnAdvancedMsgListener
    // ... more listeners
    
    // Communication channels
    conversationCh chan common.Cmd2Value     // Conversation updates
    cmdWsCh        chan common.Cmd2Value     // WebSocket commands
    msgSyncerCh    chan common.Cmd2Value     // Message sync events
    loginMgrCh     chan common.Cmd2Value     // Login manager commands
    
    // State management
    loginStatus    int                       // Logging, Logged, LogoutStatus
    loginUserID    string
    token          string
    ctx            context.Context
    cancel         context.CancelFunc
}
```

---

## Key Components

### 1. open_im_sdk/ - Public SDK Interface

**Files:**
- `init_login.go`: InitSDK, Login, Logout, NetworkStatusChanged
- `caller.go`: Reflection-based function calling system for cross-language bindings
- `apicb.go`: API error callback handling (token expiration, kicked offline)
- `userRelated.go`: LoginMgr definition and lifecycle
- `em.go`, `group.go`, `conversation_msg.go`, etc.: API aggregation

**Responsibility:**
- Provide C-compatible function exports
- Reflection-based parameter marshaling/unmarshaling (JSON ↔ Go types)
- Callback invocation and error handling
- Resource lifecycle checks

**Key Patterns:**
- `call()`: Async callbacks for long-running operations
- `syncCall()`: Synchronous calls returning JSON strings
- `messageCall()`: Message-specific async calls with progress tracking

### 2. internal/ - Business Logic Layer

#### internal/interaction/
**WebSocket Connection Management**

- `long_conn_mgr.go`: Central connection manager
  - Manages WebSocket lifecycle (connect, disconnect, reconnect)
  - Handles read/write pumps with goroutines
  - Implements ping/pong heartbeat
  - Exponential backoff reconnection strategy
  - Message compression (gzip)
  - Message encoding (GOB protocol)

- `long_connection.go`: LongConn interface (abstracts WS/TCP)
  
- `msg_sync.go`: Message synchronization orchestrator
  - Tracks max SEQ per conversation
  - Handles incremental sync on network changes
  - Manages app foreground/background state
  - Pulls message gaps using SEQ numbers
  
- `ws_resp_asyn.go`: Asynchronous response tracking
  - Maps request IDs to response channels
  - Handles timeouts for waiting responses

#### internal/conversation_msg/
**Conversation & Message Management**

- `conversation.go`: Conversation operations (get, set, search)
- `conversation_msg.go`: Message CRUD operations
- `create_message.go`: Message creation utilities for all types
- `incremental_sync.go`: SEQ-based incremental sync
- `message_check.go`: Message validation and processing

#### internal/group/, internal/user/, internal/relation/
- **group/**: Group creation, membership, permissions
- **user/**: User profile, status, subscriptions
- **relation/**: Friend requests, blacklist, relationship management

#### internal/third/
- File uploads/downloads
- Third-party API calls (FCM tokens, etc.)

### 3. pkg/ - Shared Utilities

#### pkg/db/ & pkg/db/db_interface/
**Database Layer Architecture**

Database interface composition:
```go
type DataBase interface {
    Close(ctx context.Context) error
    InitDB(ctx context.Context, userID string, dataDir string) error
    GroupModel           // Group CRUD operations
    MessageModel         // Message CRUD operations
    ConversationModel    // Conversation CRUD operations
    UserModel            // User CRUD operations
    FriendModel          // Friend & blacklist CRUD
    S3Model              // File upload tracking
    SendingMessagesModel // Retry sending messages
    VersionSyncModel     // Version tracking for sync
    AppSDKVersion        // SDK installation status
    TableMaster          // Database schema
}
```

**Key Models:**
- `LocalChatLog`: Message persistence
- `LocalConversation`: Conversation metadata
- `LocalUser`: User info
- `LocalFriend`: Friend relationships
- `LocalGroup`, `LocalGroupMember`: Group data
- `LocalBlack`: Blacklist entries

**Implementation:**
- SQLite with GORM ORM
- Per-user database files in `dataDir/{userID}/`
- Automatic schema creation via `db_init.go`

#### pkg/cache/
**Generic In-Memory Cache**

```go
type Cache[K comparable, V any] struct {
    m sync.Map  // Thread-safe map
}
```

**Features:**
- Generic key-value caching
- Thread-safe operations
- Load, Store, Delete, Range operations
- Used for quick lookups (conversations, users, groups)

#### pkg/api/
**REST API Client**

Protocol Buffer message definitions for:
- User operations (GetUserInfo, SetUserInfo)
- Message operations (SendMessage, RevokeMessage)
- Group operations (CreateGroup, JoinGroup)
- Conversation operations (SetConversations)
- Friend operations (AddFriend, DeleteFriend)

#### pkg/syncer/
**Message Synchronization Engine**

- Tracks conversation SEQ numbers (server-side message ordering)
- Manages incremental sync on reconnection
- Handles message gap detection and pulling
- Processes batch message updates

---

## Initialization & Login Flow

### SDK Initialization Sequence

```
1. InitSDK(listener, config)
   ├─ Parse IMConfig (API addr, WS addr, platform ID, etc.)
   ├─ Initialize logger
   ├─ Create LoginMgr singleton
   └─ Store listener reference

2. Login(userID, token)
   ├─ Initialize database (SQLite per user)
   ├─ Load SEQ state from database
   ├─ Create infrastructure components:
   │  ├─ Conversation manager
   │  ├─ Group manager
   │  ├─ User manager
   │  ├─ Relation (friendship) manager
   │  ├─ File manager
   │  └─ WebSocket connection manager
   ├─ Start message synchronization
   ├─ Establish WebSocket connection
   └─ Trigger OnConnectSuccess() callback

3. Logout()
   ├─ Close WebSocket connection
   ├─ Cancel all goroutines
   ├─ Close database
   └─ Cleanup listeners
```

**Key State Transitions:**
- `LogoutStatus` → `Logging` → `Logged` (or `LogoutStatus` on error)
- `CheckResourceLoad()` validates logged-in state for all operations

---

## WebSocket & Connection Management

### Connection Lifecycle

**LongConnMgr** manages persistent WebSocket connection with:

1. **Connection Establishment** (`reConn`)
   - Dial WS server with authentication
   - Set read/write deadlines
   - Configure ping/pong handlers

2. **Pump Architecture** (3 concurrent goroutines)
   - **readPump**: Reads from WS, deserializes messages, routes to handlers
   - **writePump**: Writes queued messages from `send` channel
   - **heartbeat**: Sends ping frames periodically (every 24 seconds)

3. **Automatic Reconnection**
   - Exponential backoff strategy
   - Max 300 reconnection attempts
   - Triggers message sync on reconnection

4. **Compression & Encoding**
   - Incoming: gzip decompress → GOB decode → proto.Unmarshal
   - Outgoing: proto.Marshal → GOB encode → gzip compress
   - Binary protocol only (no text protocol)

### Message Flow on WS

```go
// Request-Response Model
Message struct {
    Message GeneralWsReq     // Protobuf-encoded request
    Resp    chan *GeneralWsResp  // Channel for async response
}

// GeneralWsReq: Contains reqIdentifier, userID, operationID, data
// GeneralWsResp: Contains ErrCode, ErrMsg, Data (proto-encoded response)
```

### Error Handling Callbacks

In `apicb.go`, API errors trigger SDK callbacks:
- **TokenExpiredError** → `OnUserTokenExpired()`
- **TokenInvalidError** → `OnUserTokenInvalid()`
- **TokenKickedError** → `OnKickedOffline()`

Uses atomic compare-and-swap to ensure single trigger.

---

## Message Synchronization

### MsgSyncer Architecture

**MsgSyncer** is a central hub for message sync:

```go
type MsgSyncer struct {
    loginUserID       string
    longConnMgr       *LongConnMgr          // For sending/receiving
    recvCh            chan common.Cmd2Value // Push notifications
    conversationCh    chan common.Cmd2Value // Updates to conversations
    syncedMaxSeqs     map[string]int64      // Per-conversation SEQ tracking
    db                db_interface.DataBase
    reinstalled       bool                  // App reinstall flag
    isSyncing         bool                  // Sync state
}
```

### Sync Triggers

1. **On Login**: Full sync or incremental based on `reinstalled` flag
2. **On Network Reconnection**: Incremental sync of new messages
3. **On App Foreground**: Resume message pulling
4. **On Push Notification**: Sync specified conversation
5. **Periodic**: Background sync (configurable)

### Sync Algorithm

```
For each conversation:
    1. Read lastSyncedSeq from memory/database
    2. Request messages with seq > lastSyncedSeq
    3. Batch pull (up to 100 messages per request)
    4. Insert into local database
    5. Trigger OnRecvNewMessage callbacks
    6. Update SEQ and conversation lastMessage
```

### SEQ Number Handling

- **Server-side SEQ**: Global ordering of messages in conversation
- **Local max SEQ**: Highest SEQ received locally per conversation
- **SEQ gaps**: Detected and filled via pull requests
- **Persistence**: SEQ state saved to database for resumption

---

## Database & Cache Layer

### SQLite Database Structure

**Per-user database** located at: `{dataDir}/{userID}/im_{platformID}.db`

**Tables:**
- `local_chat_log`: Messages with SEQ, status, local extensions
- `local_conversation`: Conversation metadata, draft, unread count
- `local_friend`: Friend list and relationships
- `local_friend_request`: Pending friend requests
- `local_black`: Blacklisted users
- `local_group`: Group information
- `local_group_member`: Group membership
- `local_user`: Login user profile
- `local_upload`: File upload tracking
- `local_version_sync`: Entity version tracking for sync
- `local_app_sdk_version`: SDK installation metadata

### Database Access Pattern

**GORM ORM over SQLite:**
- Model structs in `pkg/db/model_struct/`
- Interface-based design in `pkg/db/db_interface/`
- Transactional operations with context
- Automatic error handling

**Initialization:**
```go
db.NewDataBase(ctx, userID, dataDir, logLevel)
// Creates per-user database file if not exists
```

### Cache Strategy

**Two-tier caching:**
1. **Database**: Persistent storage via SQLite
2. **Memory**: `Cache[K, V]` for frequently accessed items
   - Conversations
   - Users (in group context)
   - Friends
   - Groups

**Cache invalidation:** On-demand by push notifications, sync updates

---

## Callback & Event System

### Callback Interfaces (in open_im_sdk_callback/)

**Connection Events:**
- `OnConnListener`: OnConnecting, OnConnectSuccess, OnConnectFailed, OnKickedOffline, OnUserTokenExpired/Invalid

**Message Events:**
- `OnAdvancedMsgListener`: OnRecvNewMessage, OnRecvC2CReadReceipt, OnNewRecvMessageRevoked, etc.
- `OnBatchMsgListener`: OnRecvNewMessages (batch of multiple messages)

**Conversation Events:**
- `OnConversationListener`: OnSyncServerStart/Finish/Progress, OnNewConversation, OnConversationChanged, OnTotalUnreadMessageCountChanged

**User Events:**
- `OnUserListener`: OnSelfInfoUpdated, OnUserStatusChanged, OnUserCommand*
- `OnFriendshipListener`: OnFriendAdded/Deleted/InfoChanged, OnBlackAdded/Deleted, etc.
- `OnGroupListener`: OnJoinedGroupAdded/Deleted, OnGroupMemberAdded/Deleted, etc.

### Event Flow

```
WebSocket receives notification
    ↓
readPump in LongConnMgr
    ↓
handleMessage (deserialize protobuf)
    ↓
Route by message type (push, sync, etc.)
    ↓
Update database
    ↓
Trigger appropriate OnXxxListener callback
```

### Callback Patterns

**Go SDK:**
- Direct function calls on listener interfaces
- Async invocation via goroutines
- Error handling with CodeError

**WASM (JavaScript):**
- Callbacks marshaled to JavaScript
- Event names (camelCase function names)
- Data serialized to JSON strings
- Global callback registry via `js.Global()`

---

## Cross-Platform Architecture

### Platform Abstraction

1. **Network Layer**: `LongConn` interface allows TCP or WebSocket
   - `ws_default.go`: gorilla/websocket (for native platforms)
   - `ws_js.go`: WASM WebSocket (for browser)

2. **Database Layer**: `DataBase` interface
   - `db/`: SQLite via GORM (iOS, Android, PC)
   - `wasm/indexdb/`: IndexedDB (WASM) with same interface

3. **Function Calling**: Reflection-based (Go) and JavaScript (WASM)
   - `caller.go`: Handles type conversions, JSON marshaling
   - `wasm/event_listener/caller.go`: JavaScript/Go bridge

### Platform-Specific Builds

**Build Tags:**
- `//go:build js && wasm`: WASM-specific code
- `//go:build !js || !wasm`: Native builds

**WASM Entry Point:** `wasm/cmd/main.go` registers all SDK functions to global JavaScript scope

---

## WASM Implementation

### WASM-Specific Components

1. **IndexedDB Database** (`wasm/indexdb/`)
   - `init.go`: IndexedDB initialization
   - Model files: `*_model.go` implementing DataBase interface
   - Same schema as SQLite version
   - Async JavaScript calls via channel bridging

2. **WASM Wrapper** (`wasm/wasm_wrapper/`)
   - `wasm_init_login.go`: InitSDK, Login, Logout
   - `wasm_conversation_msg.go`: Message and conversation operations
   - `wasm_group.go`: Group operations
   - `wasm_user.go`: User operations
   - `wasm_friend.go`: Friend operations
   - `wasm_third.go`: File uploads

3. **Event Listener** (`wasm/event_listener/`)
   - `listener.go`: Bridges SDK callbacks to JavaScript
   - `callback_writer.go`: Marshals events to JSON for JS
   - `caller.go`: Handles async JS function calls

### JavaScript API (from WASM)

**Global Functions Registered:**
- `initSDK(config, commonEventFunc)`: Initialize SDK
- `login(userID, token)`, `logout()`: Authentication
- `createTextMessage()`, `sendMessage()`: Messages
- `createGroup()`, `joinGroup()`: Groups
- `getFriendList()`, `addFriend()`: Friends
- `getLoginStatus()`, `setAppBackgroundStatus()`: Status
- ~100+ functions total

**Event Callback:**
- `commonEventFunc(eventName, eventData)`: Single entry point for all events

**Usage Pattern:**
```javascript
await initSDK(config, (eventName, data) => {
    console.log(eventName, data);  // e.g., "onConnectSuccess", "{}"
});
await login(userID, token);
```

### IndexedDB Schema

Same tables as SQLite for consistency:
- Async JavaScript operations wrapped in Go channels
- Maintains same query semantics
- Automatic transaction handling

---

## Communication Channels (Actor Pattern)

**Channel-based communication** between components:

1. **conversationCh**: Conversation updates
   - Sent by: Conversation manager, message sync
   - Received by: LoginMgr listeners

2. **cmdWsCh**: WebSocket commands
   - Sent by: Message sync, user actions
   - Received by: LongConnMgr

3. **msgSyncerCh**: Message sync events
   - Sent by: LongConnMgr (push notifications)
   - Received by: MsgSyncer

4. **loginMgrCh**: Login state commands
   - Sent by: Error handlers (token expiration)
   - Received by: LoginMgr (triggers logout)

**Channel Type:**
```go
type Cmd2Value struct {
    Cmd   int         // Command type identifier
    Value interface{} // Command data
}
```

---

## Error Handling Strategy

### Error Types (pkg/sdkerrs/)

```go
ErrArgs              // Invalid arguments
ErrSdkInternal       // Internal SDK error
ErrResourceLoad      // SDK not initialized
ErrCtxDeadline       // Context timeout
```

### Error Propagation

1. **API Errors**: HTTP error codes → CodeError → callback.OnError(code, msg)
2. **WS Errors**: Connection errors → auto-reconnect → sync on success
3. **DB Errors**: Query failures → error context → propagate up
4. **Token Errors**: Token expiration → OnUserTokenExpired callback → force logout

### Atomic State Management

Uses `sync.atomic` for thread-safe state flags:
- `tokenExpiredState`: Ensures OnUserTokenExpired fires once
- `kickedOfflineState`: Ensures OnKickedOffline fires once
- `loginStatus`: Protected by mutex for consistency

---

## Key Architectural Patterns

### 1. Dependency Injection
- Components receive their dependencies (db, listeners, channels)
- Makes testing and mocking easier
- Reduces tight coupling

### 2. Interface-Based Design
- Database: `db_interface.DataBase`
- Network: `interaction.LongConn`
- Callbacks: Multiple listener interfaces
- Enables cross-platform implementations

### 3. Context-Driven Lifecycle
- All operations use `context.Context`
- Goroutines respect context cancellation
- Timeout management at operation level

### 4. Protobuf for Serialization
- Compact binary format
- Cross-language compatibility
- Version evolution support

### 5. SEQ-Based Message Ordering
- Server-assigned SEQ numbers per conversation
- Enables incremental sync
- Detects message gaps reliably

### 6. Reflection-Based RPC (for Go bindings)
- Dynamic type conversion
- JSON marshaling for cross-language calls
- Async and sync call patterns

---

## Performance Considerations

1. **Message Batching**: Sync pulls up to 100 messages per request
2. **Compression**: gzip compression on WebSocket for bandwidth optimization
3. **Local Caching**: Frequent lookups use in-memory Cache
4. **Incremental Sync**: Only new messages pulled via SEQ tracking
5. **Exponential Backoff**: Reconnection delays prevent server overload
6. **Goroutine Management**: Limited concurrency (10 pullMsg goroutines max)

---

## Development Notes

### Adding New Features

1. **New API Operation**:
   - Add protobuf message in `pkg/api/`
   - Implement in appropriate domain manager (conversation, user, group, etc.)
   - Add public function in `open_im_sdk/` layer
   - Register callback listener if needed

2. **New Event Type**:
   - Add interface method to listener
   - Trigger from appropriate handler
   - Wire callback in LoginMgr
   - For WASM: register in `wasm_wrapper/` and `event_listener/`

3. **Database Schema Change**:
   - Modify model struct in `pkg/db/model_struct/`
   - Update all interface methods in `db_interface/`
   - Implement in SQLite layer
   - For WASM: update IndexedDB layer

### Testing Approach

- Unit tests use interface mocking
- Integration tests with real database (SQLite)
- WebSocket testing with mock server
- Protocol conversion testing (Protobuf ↔ JSON)

---

## Summary

The openim-sdk-core is a well-architected, layered IM SDK that:

- **Separates concerns** into public API, business logic, and utilities
- **Abstracts platform differences** through interfaces (WS, database, callbacks)
- **Uses efficient protocols** (Protobuf, gzip, binary WebSocket)
- **Manages state carefully** with channels and atomic operations
- **Supports multiple platforms** (iOS/Android/Web/PC) from single codebase
- **Implements robust sync** with SEQ-based incremental updates
- **Provides rich callbacks** for real-time event notifications

The design enables easy maintenance, testing, and extension while maintaining cross-platform compatibility.

---

## Q&A / Knowledge Base for Java Developers

This section explains key concepts in this codebase for developers coming from a Java background who are new to Go.

### WebSocket Management Deep Dive

#### Q1: How does the WebSocket architecture work in this project?

**Overview:**

The WebSocket implementation in `internal/interaction/` uses a pattern called "pumps" - separate goroutines (lightweight threads) for reading, writing, and heartbeats. Think of it like having three dedicated threads in Java's `ExecutorService`, but much more lightweight.

**Key Files:**
- `long_conn_mgr.go` - Main connection manager (like a Java `ConnectionManager` class)
- `ws_default.go` - WebSocket wrapper using gorilla/websocket library
- `ws_resp_asyn.go` - Async request-response tracking (like Java's `CompletableFuture` pattern)
- `reconnect.go` - Exponential backoff reconnection strategy
- `encoder.go` - GOB encoding (similar to Java serialization)
- `compressor.go` - Gzip compression with object pooling

**The Three-Pump Architecture:**

```
┌─────────────────────────────────────────────┐
│          LongConnMgr (Main Manager)         │
│                                             │
│  ┌───────────┐  ┌───────────┐  ┌─────────┐│
│  │ readPump  │  │writePump  │  │heartbeat││
│  │(goroutine)│  │(goroutine)│  │(gorout.)││
│  └─────┬─────┘  └─────▲─────┘  └─────┬───┘│
│        │              │              │    │
└────────┼──────────────┼──────────────┼────┘
         │              │              │
         ▼              │              ▼
   WebSocket.ReadMessage │        Send Ping
         │              │
         │         send channel
         │         (buffered)
         │              │
         ▼              │
   handleMessage ───────┘
```

**Java Comparison:**
```java
// In Java, you might write:
ExecutorService executor = Executors.newFixedThreadPool(3);
executor.submit(() -> readLoop());
executor.submit(() -> writeLoop());
executor.submit(() -> heartbeatLoop());

// In Go, it's:
go c.readPump(ctx)    // goroutine: lightweight thread
go c.writePump(ctx)   // goroutine
go c.heartbeat(ctx)   // goroutine
```

---

#### Q2: What are goroutines and how do they compare to Java threads?

**Goroutines vs Java Threads:**

| Aspect | Java Thread | Go Goroutine |
|--------|-------------|--------------|
| **Memory** | 1-2 MB stack | 2 KB initial stack (grows dynamically) |
| **Creation** | Expensive (OS thread) | Very cheap (user-space) |
| **Scheduling** | OS kernel scheduler | Go runtime scheduler (M:N mapping) |
| **Cost** | ~1000 threads max | Millions possible |
| **Syntax** | `new Thread(() -> {...}).start()` | `go func() {...}()` |

**Example from long_conn_mgr.go:132-136:**
```go
func (c *LongConnMgr) Run(ctx context.Context) {
    go c.readPump(ctx)      // Start reading messages
    go c.writePump(ctx)     // Start writing messages
    go c.heartbeat(ctx)     // Start heartbeat
}
```

This creates 3 concurrent goroutines. In Java, this would require:
```java
public void run(Context ctx) {
    new Thread(() -> readPump(ctx)).start();
    new Thread(() -> writePump(ctx)).start();
    new Thread(() -> heartbeat(ctx)).start();
}
```

But goroutines are ~500x cheaper in memory!

---

#### Q3: What are channels and how do they compare to Java's BlockingQueue?

**Channels are Go's way of communicating between goroutines.**

**Java Pattern:**
```java
// Java: Using BlockingQueue
BlockingQueue<Message> queue = new LinkedBlockingQueue<>(10);

// Producer thread
queue.put(message);  // Blocks if full

// Consumer thread
Message msg = queue.take();  // Blocks if empty
```

**Go Pattern (from long_conn_mgr.go:126):**
```go
// Go: Using channels
c.send = make(chan Message, 10)  // Buffered channel (size 10)

// Producer goroutine
c.send <- message  // Blocks if channel is full

// Consumer goroutine (long_conn_mgr.go:264)
message := <-c.send  // Blocks if channel is empty
```

**Channel in writePump (long_conn_mgr.go:258-297):**
```go
func (c *LongConnMgr) writePump(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():           // Context cancellation (like Thread.interrupt())
            return
        case message, ok := <-c.send: // Receive from channel
            if !ok {                  // Channel closed
                return
            }
            // Process message...
        }
    }
}
```

**Java equivalent using BlockingQueue:**
```java
public void writePump(Context ctx) {
    while (!Thread.interrupted()) {
        try {
            Message message = sendQueue.poll(100, TimeUnit.MILLISECONDS);
            if (message != null) {
                // Process message...
            }
        } catch (InterruptedException e) {
            break;
        }
    }
}
```

**Key Differences:**
- Channels are language primitives in Go (keywords: `chan`, `<-`)
- Channels can be closed to signal "no more data"
- `select` statement allows waiting on multiple channels simultaneously (like Java's `Selector` for I/O)

---

#### Q4: How does the readPump work? (Message Reading)

**Location:** `long_conn_mgr.go:177-236`

**Flow:**
```
┌──────────────────────────────────────────┐
│ 1. Check if connected, reconnect if not │
│         ↓                                │
│ 2. Read binary message from WebSocket   │
│         ↓                                │
│ 3. Decompress (gzip)                    │
│         ↓                                │
│ 4. Decode (GOB → GeneralWsResp struct)  │
│         ↓                                │
│ 5. Route by ReqIdentifier:              │
│    - PushMsg: Forward to message syncer │
│    - LogoutMsg: Trigger logout          │
│    - SendMsg/GetSeq: Notify waiting ch  │
│         ↓                                │
│ 6. Loop forever (until ctx.Done())      │
└──────────────────────────────────────────┘
```

**Code walkthrough:**
```go
func (c *LongConnMgr) readPump(ctx context.Context) {
    defer func() {
        if r := recover(); r != nil {  // Catch panics (like Java try-catch)
            err := fmt.Sprintf("panic: %+v\n%s", r, debug.Stack())
            log.ZWarn(ctx, "readPump panic", nil, "panic info", err)
        }
    }()

    for {
        select {
        case <-ctx.Done():  // Context cancelled (logout)
            return
        default:
        }

        // Reconnect if needed
        needRecon, err := c.reConn(ctx, &connNum)
        if err != nil {
            time.Sleep(c.reconnectStrategy.GetSleepInterval())
            continue
        }

        // Read from WebSocket
        messageType, message, err := c.conn.ReadMessage()
        if err != nil {
            _ = c.close()
            continue
        }

        // Only process binary messages
        switch messageType {
        case MessageBinary:
            err := c.handleMessage(message)  // Decompress, decode, route
        case MessageText:
            return  // Not supported
        case CloseMessage:
            return
        }
    }
}
```

**Java equivalent structure:**
```java
public void readPump(Context ctx) {
    try {
        while (!ctx.isCancelled()) {
            if (!isConnected()) {
                reconnect();
                continue;
            }

            byte[] message = websocket.readBinaryMessage();
            if (message == null) {
                close();
                continue;
            }

            handleMessage(message);
        }
    } catch (Exception e) {
        log.error("readPump error", e);
    }
}
```

**Key Go Concepts:**
- `defer`: Executes function when surrounding function returns (like Java's `finally`)
- `recover()`: Catches panics (like Java's `catch (Throwable)`)
- `select` with `default`: Non-blocking check of context cancellation

---

#### Q5: How does the writePump work? (Message Sending)

**Location:** `long_conn_mgr.go:243-298`

**Flow:**
```
┌───────────────────────────────────────┐
│ 1. Wait for message on send channel   │
│         ↓                             │
│ 2. Send message over WebSocket and    │
│    wait for response with timeout     │
│         ↓                             │
│ 3. Receive response (or timeout)      │
│         ↓                             │
│ 4. Notify waiting channel (Resp chan) │
│         ↓                             │
│ 5. Loop forever                       │
└───────────────────────────────────────┘
```

**Code:**
```go
func (c *LongConnMgr) writePump(ctx context.Context) {
    defer func() {
        close(c.send)  // Close channel when done
    }()

    for {
        select {
        case <-ctx.Done():
            return

        case message, ok := <-c.send:  // Receive from send channel
            if !ok {  // Channel was closed
                _ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }

            // Send and wait for response (with retry logic)
            resp, err := c.sendAndWaitResp(&message.Message)

            // Notify the goroutine waiting for this response
            c.Syncer.notifyCh(message.Resp, resp, 1)
        }
    }
}
```

**This pattern implements async request-response over WebSocket:**

```
Client goroutine:               writePump:                readPump:
     │                              │                         │
     │  1. Create Message           │                         │
     │     with Resp channel        │                         │
     │                              │                         │
     │  2. Send to send channel     │                         │
     ├─────────────────────────────>│                         │
     │                              │                         │
     │                              │ 3. Write to WebSocket   │
     │                              ├────────────────────────>│
     │                              │                         │
     │  BLOCK waiting on            │                         │
     │  message.Resp channel        │                         │
     │                              │                    4. Response arrives
     │                              │<────────────────────────┤
     │                              │                         │
     │                              │ 5. Route to Syncer     │
     │                              │                         │
     │  6. Receive from Resp chan   │                         │
     │<─────────────────────────────┤                         │
     │                              │                         │
     │  7. Return result            │                         │
```

**Java equivalent using CompletableFuture:**
```java
public CompletableFuture<Response> sendMessage(Message msg) {
    CompletableFuture<Response> future = new CompletableFuture<>();

    RequestMessage request = new RequestMessage(msg, future);
    sendQueue.offer(request);

    return future;
}

// In write thread:
while (true) {
    RequestMessage request = sendQueue.take();
    String msgIncr = generateMsgId();
    pendingResponses.put(msgIncr, request.future);

    websocket.send(request.message);
}

// In read thread:
while (true) {
    Response response = websocket.receive();
    CompletableFuture<Response> future = pendingResponses.remove(response.msgIncr);
    if (future != null) {
        future.complete(response);
    }
}
```

---

#### Q6: How does heartbeat/ping-pong work?

**Location:** `long_conn_mgr.go:300-326`

**WebSocket Ping-Pong Mechanism:**
```
Client                          Server
  │                               │
  ├─────── PING (every 24s) ─────>│
  │                               │
  │<────── PONG ──────────────────┤
  │                               │
  │  (Reset read deadline)        │
  │                               │
```

**Constants (long_conn_mgr.go:47-54):**
```go
const (
    writeWait  = 10 * time.Second  // Timeout for writes
    pongWait   = 30 * time.Second  // Expect pong within 30s
    pingPeriod = (pongWait * 8) / 10  // Send ping every 24s
)
```

**Heartbeat goroutine:**
```go
func (c *LongConnMgr) heartbeat(ctx context.Context) {
    ticker := time.NewTicker(pingPeriod)  // Tick every 24 seconds
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:  // Every 24 seconds
            c.sendPingMessage(ctx)
        }
    }
}

func (c *LongConnMgr) sendPingMessage(ctx context.Context) {
    c.connWrite.Lock()
    defer c.connWrite.Unlock()

    if c.IsConnected() {
        c.conn.SetWriteDeadline(writeWait)
        c.conn.WriteMessage(PingMessage, []byte(operationID))
    }
}
```

**Pong handler (long_conn_mgr.go:743-749):**
```go
func (c *LongConnMgr) pongHandler(appData string) error {
    // Reset read deadline - we got a pong, connection is alive
    return c.conn.SetReadDeadline(pongWait)
}
```

**Java equivalent:**
```java
// Using ScheduledExecutorService
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

scheduler.scheduleAtFixedRate(() -> {
    if (isConnected()) {
        synchronized (connLock) {
            websocket.sendPing(operationId.getBytes());
        }
    }
}, 0, 24, TimeUnit.SECONDS);

// Pong handler
websocket.setPongHandler((pongData) -> {
    // Reset read timeout
    websocket.setReadTimeout(30_000);
});
```

**Why ping-pong?**
- Detects dead connections (server crash, network issue)
- Keeps NAT/firewall mappings alive
- If no pong received in 30s, read deadline expires → reconnect

---

#### Q7: How does message encoding/compression work?

**Pipeline (long_conn_mgr.go:436-454):**

**Outgoing:**
```
Go Struct (GeneralWsReq)
    ↓
GOB Encode (like Java serialization)
    ↓
Gzip Compress (with pool)
    ↓
Binary WebSocket Frame
```

**Incoming (long_conn_mgr.go:467-482):**
```
Binary WebSocket Frame
    ↓
Gzip Decompress (with pool)
    ↓
GOB Decode
    ↓
Go Struct (GeneralWsResp)
```

**Encoding - GOB (encoder.go:34-41):**
```go
func (g *GobEncoder) Encode(data interface{}) ([]byte, error) {
    buff := bytes.Buffer{}
    enc := gob.NewEncoder(&buff)
    err := enc.Encode(data)
    if err != nil {
        return nil, err
    }
    return buff.Bytes(), nil
}
```

**Java equivalent:**
```java
// Using Java Serialization
ByteArrayOutputStream baos = new ByteArrayOutputStream();
ObjectOutputStream oos = new ObjectOutputStream(baos);
oos.writeObject(data);
return baos.toByteArray();
```

**Compression with Object Pooling (compressor.go:58-72):**
```go
var gzipWriterPool = sync.Pool{
    New: func() any { return gzip.NewWriter(nil) }
}

func (g *GzipCompressor) CompressWithPool(rawData []byte) ([]byte, error) {
    gz := gzipWriterPool.Get().(*gzip.Writer)
    defer gzipWriterPool.Put(gz)  // Return to pool

    gzipBuffer := bytes.Buffer{}
    gz.Reset(&gzipBuffer)
    gz.Write(rawData)
    gz.Close()
    return gzipBuffer.Bytes(), nil
}
```

**Why object pooling?**
- Creating gzip writers is expensive (allocates buffers)
- `sync.Pool` reuses objects (like Apache Commons Pool in Java)
- Reduces GC pressure

**Java equivalent:**
```java
// Using Apache Commons Pool
ObjectPool<GZIPOutputStream> gzipPool = new GenericObjectPool<>(
    new BasePooledObjectFactory<GZIPOutputStream>() {
        @Override
        public GZIPOutputStream create() {
            return new GZIPOutputStream(new ByteArrayOutputStream());
        }
    }
);

GZIPOutputStream gz = gzipPool.borrowObject();
try {
    // Use gz...
} finally {
    gzipPool.returnObject(gz);
}
```

---

#### Q8: How does async request-response tracking work?

**The Problem:**
WebSocket is bidirectional streaming. How do you match responses to requests?

**Solution: WsRespAsyn (ws_resp_asyn.go)**

**Data Structure:**
```go
type WsRespAsyn struct {
    wsNotification map[string]chan *GeneralWsResp  // Like Map<String, CompletableFuture>
    wsMutex        sync.RWMutex                    // Read-write lock
}
```

**Flow:**

1. **Request side - AddCh (ws_resp_asyn.go:59-71):**
```go
func (u *WsRespAsyn) AddCh(userID string) (string, chan *GeneralWsResp) {
    u.wsMutex.Lock()
    defer u.wsMutex.Unlock()

    msgIncr := GenMsgIncr(userID)  // Generate unique ID: "userID_timestamp_random"
    ch := make(chan *GeneralWsResp, 1)  // Buffered channel (size 1)
    u.wsNotification[msgIncr] = ch
    return msgIncr, ch
}
```

2. **Send request with msgIncr (long_conn_mgr.go:138-169):**
```go
func (c *LongConnMgr) SendReqWaitResp(ctx context.Context, m proto.Message,
                                       reqIdentifier int, resp proto.Message) error {
    // Create message with unique msgIncr
    msgIncr, tempChan := c.Syncer.AddCh(userID)

    msg := Message{
        Message: GeneralWsReq{
            ReqIdentifier: reqIdentifier,
            MsgIncr:       msgIncr,  // Unique ID
            Data:          protoData,
        },
        Resp: tempChan,  // Channel to receive response
    }

    c.send <- msg  // Send to writePump

    // BLOCK waiting for response
    select {
    case <-ctx.Done():
        return sdkerrs.ErrCtxDeadline
    case v := <-msg.Resp:  // Wait for response on this channel
        return proto.Unmarshal(v.Data, resp)
    }
}
```

3. **Response arrives - NotifyResp (ws_resp_asyn.go:119-136):**
```go
func (u *WsRespAsyn) NotifyResp(ctx context.Context, wsResp GeneralWsResp) error {
    u.wsMutex.Lock()
    defer u.wsMutex.Unlock()

    ch := u.GetCh(wsResp.MsgIncr)  // Find waiting channel by msgIncr
    if ch == nil {
        return errors.New("no channel found")
    }

    ch <- &wsResp  // Send response to waiting goroutine
    return nil
}
```

**Sequence Diagram:**
```
Caller goroutine          WsRespAsyn          writePump          readPump
     │                        │                    │                  │
     │ SendReqWaitResp()      │                    │                  │
     ├──────────────────────> │                    │                  │
     │                        │                    │                  │
     │                    AddCh(userID)            │                  │
     │                  returns (msgIncr, chan)    │                  │
     │                        │                    │                  │
     │   Send Message with msgIncr                 │                  │
     ├────────────────────────────────────────────>│                  │
     │                        │                    │                  │
     │   BLOCK on chan        │                    │                  │
     │   <-msg.Resp           │              Write to WS              │
     │                        │                    ├─────────────────>│
     │                        │                    │                  │
     │                        │                    │   Response arrives
     │                        │                    │<─────────────────┤
     │                        │                    │                  │
     │                        │   NotifyResp(msgIncr, response)       │
     │                        │<───────────────────┼──────────────────┤
     │                        │                    │                  │
     │                     ch <- response          │                  │
     │   UNBLOCK              │                    │                  │
     │<───────────────────────│                    │                  │
     │                        │                    │                  │
     │   Process response     │                    │                  │
```

**Java equivalent:**
```java
// Using CompletableFuture
public class WsRespAsyn {
    private Map<String, CompletableFuture<GeneralWsResp>> pending = new ConcurrentHashMap<>();

    public String addFuture(String userID) {
        String msgIncr = generateMsgIncr(userID);
        CompletableFuture<GeneralWsResp> future = new CompletableFuture<>();
        pending.put(msgIncr, future);
        return msgIncr;
    }

    public void notifyResp(GeneralWsResp response) {
        CompletableFuture<GeneralWsResp> future = pending.remove(response.msgIncr);
        if (future != null) {
            future.complete(response);
        }
    }
}

// Usage:
public GeneralWsResp sendReqWaitResp(Message msg) throws Exception {
    String msgIncr = syncer.addFuture(userID);
    msg.setMsgIncr(msgIncr);

    sendQueue.offer(msg);

    CompletableFuture<GeneralWsResp> future = syncer.getFuture(msgIncr);
    return future.get(10, TimeUnit.SECONDS);  // Block with timeout
}
```

---

#### Q9: How does reconnection work?

**Reconnection Strategy (reconnect.go):**

```go
type ExponentialRetry struct {
    attempts []int  // [1, 2, 4, 8, 16]
    index    int
}

func (rs *ExponentialRetry) GetSleepInterval() time.Duration {
    rs.index++
    interval := rs.index % len(rs.attempts)
    return time.Second * time.Duration(rs.attempts[interval])
}
```

**Backoff sequence:** 1s → 2s → 4s → 8s → 16s → 1s → 2s → ... (cycles)

**Reconnection Logic (long_conn_mgr.go:629-699):**
```go
func (c *LongConnMgr) reConn(ctx context.Context, num *int) (needRecon bool, err error) {
    if c.IsConnected() {
        return true, nil  // Already connected
    }

    c.listener.OnConnecting()  // Callback: connecting
    c.SetConnectionStatus(Connecting)

    // Build WebSocket URL with auth params
    url := fmt.Sprintf("%s?sendID=%s&token=%s&platformID=%d&compression=gzip",
        wsAddr, userID, token, platformID)

    resp, err := c.conn.Dial(url, nil)
    if err != nil {
        c.SetConnectionStatus(Closed)

        // Check error type
        if resp != nil {
            var apiResp struct {
                ErrCode int    `json:"errCode"`
                ErrMsg  string `json:"errMsg"`
            }
            json.Unmarshal(body, &apiResp)

            // Fatal errors - don't retry
            switch apiResp.ErrCode {
            case TokenExpiredError, TokenInvalidError:
                return false, err  // Stop reconnecting
            default:
                return true, err   // Retry
            }
        }

        c.listener.OnConnectFailed(NetworkError, err.Error())
        return true, err
    }

    c.listener.OnConnectSuccess()  // Callback: connected
    c.SetConnectionStatus(Connected)
    c.reconnectStrategy.Reset()  // Reset backoff

    // Trigger sync
    common.TriggerCmdConnected(ctx, c.pushMsgAndMaxSeqCh)

    return true, nil
}
```

**Usage in readPump (long_conn_mgr.go:201-210):**
```go
for {
    needRecon, err := c.reConn(ctx, &connNum)
    if !needRecon {  // Fatal error, stop
        return
    }
    if err != nil {  // Retryable error
        time.Sleep(c.reconnectStrategy.GetSleepInterval())  // Exponential backoff
        continue
    }

    // Connected, read messages...
}
```

**Java equivalent:**
```java
public class ExponentialBackoff {
    private int[] attempts = {1, 2, 4, 8, 16};
    private int index = 0;

    public long getSleepMs() {
        int interval = attempts[index % attempts.length];
        index++;
        return interval * 1000L;
    }
}

// In connection loop:
ExponentialBackoff backoff = new ExponentialBackoff();
while (!Thread.interrupted()) {
    try {
        if (!isConnected()) {
            reconnect();
            backoff.reset();
        }

        // Read messages...
    } catch (TokenExpiredException e) {
        break;  // Fatal, stop
    } catch (IOException e) {
        Thread.sleep(backoff.getSleepMs());
        continue;  // Retry
    }
}
```

---

#### Q10: What about thread safety and mutexes?

**Go Mutex vs Java synchronized:**

**Go:**
```go
type LongConnMgr struct {
    w          sync.Mutex    // Like Java's ReentrantLock
    connStatus int
    connWrite  *sync.Mutex   // Separate lock for writes
}

func (c *LongConnMgr) IsConnected() bool {
    c.w.Lock()
    defer c.w.Unlock()  // Unlock on return (like try-finally)
    return c.connStatus == Connected
}
```

**Java equivalent:**
```java
public class LongConnMgr {
    private final ReentrantLock lock = new ReentrantLock();
    private int connStatus;
    private final ReentrantLock writeLock = new ReentrantLock();

    public boolean isConnected() {
        lock.lock();
        try {
            return connStatus == CONNECTED;
        } finally {
            lock.unlock();
        }
    }
}
```

**Read-Write Lock (ws_resp_asyn.go:48):**
```go
type WsRespAsyn struct {
    wsNotification map[string]chan *GeneralWsResp
    wsMutex        sync.RWMutex  // Read-write lock
}

func (u *WsRespAsyn) GetCh(msgIncr string) chan *GeneralWsResp {
    u.wsMutex.RLock()  // Read lock (multiple readers allowed)
    defer u.wsMutex.RUnlock()
    return u.wsNotification[msgIncr]
}

func (u *WsRespAsyn) AddCh(userID string) (string, chan *GeneralWsResp) {
    u.wsMutex.Lock()   // Write lock (exclusive)
    defer u.wsMutex.Unlock()
    // Modify map...
}
```

**Java equivalent:**
```java
public class WsRespAsyn {
    private final Map<String, Channel> map = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public Channel getCh(String msgIncr) {
        rwLock.readLock().lock();
        try {
            return map.get(msgIncr);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String addCh(String userID) {
        rwLock.writeLock().lock();
        try {
            // Modify map...
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
```

---

#### Q11: How do I trace WebSocket issues?

**Key logging points:**

1. **Connection state changes:**
```go
// long_conn_mgr.go:695
log.ZInfo(c.ctx, "long conn establish success", "localAddr", c.conn.LocalAddr(), "connNum", *num)
```

2. **Message send/receive:**
```go
// long_conn_mgr.go:153
log.ZDebug(ctx, "send message to send channel success", "msg", m, "reqIdentifier", reqIdentifier)

// long_conn_mgr.go:483
log.ZInfo(ctx, "recv msg", "errCode", wsResp.ErrCode, "errMsg", wsResp.ErrMsg, "reqIdentifier", wsResp.ReqIdentifier)
```

3. **Reconnection attempts:**
```go
// long_conn_mgr.go:207
log.ZWarn(c.ctx, "reConn", err)
```

4. **Goroutine IDs for debugging (long_conn_mgr.go:345-354):**
```go
func getGoroutineID() int64 {
    buf := make([]byte, 64)
    buf = buf[:runtime.Stack(buf, false)]
    idField := strings.Fields(strings.TrimPrefix(string(buf), "goroutine "))[0]
    id, _ := strconv.ParseInt(idField, 10, 64)
    return id
}
```

**Search logs for:**
- "readPump start" - goroutine started
- "writePump start" - goroutine started
- "heartbeat start" - goroutine started
- "long conn establish success" - connected
- "reConn" - reconnecting
- "recv msg" - message received
- "send message to send channel success" - message sent

---

### Summary: Key Go Concepts for Java Developers

| Concept | Java | Go |
|---------|------|-----|
| **Threads** | `new Thread().start()` | `go func(){}()` |
| **Thread Communication** | `BlockingQueue` | `chan` (channel) |
| **Locks** | `synchronized`, `ReentrantLock` | `sync.Mutex` |
| **Read-Write Locks** | `ReentrantReadWriteLock` | `sync.RWMutex` |
| **Thread Pool** | `ExecutorService` | goroutines (no pool needed) |
| **Future/Promise** | `CompletableFuture` | `chan` (as future) |
| **Timer** | `ScheduledExecutorService` | `time.Ticker` |
| **Cleanup** | `try-finally` | `defer` |
| **Exception Handling** | `try-catch` | `if err != nil`, `recover()` |
| **Object Pooling** | Apache Commons Pool | `sync.Pool` |
| **Serialization** | Java Serialization | `encoding/gob` or Protocol Buffers |

**Key Takeaways:**
1. Goroutines are cheap - spawn thousands without worry
2. Channels replace most concurrent collections
3. `select` is powerful - wait on multiple channels/timeout simultaneously
4. `defer` ensures cleanup - like Java's try-finally but cleaner
5. Error handling is explicit - no exceptions (mostly)
6. Mutexes are simple - lock/unlock, use `defer` for safety
