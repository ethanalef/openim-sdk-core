# WebSocket and Connection Management

## Overview

This document details the WebSocket connection management implementation in `internal/interaction/`, focusing on connection establishment, the three-pump architecture, and reconnection strategies.

---

## Table of Contents

- [Connection Establishment Flow](#connection-establishment-flow)
  - [1. Entry Point: `reConn()` Function](#1-entry-point-reconn-function)
  - [2. Connection States & Callbacks](#2-connection-states--callbacks)
  - [3. Build WebSocket URL](#3-build-websocket-url)
  - [4. Dial WebSocket Connection](#4-dial-websocket-connection)
  - [5. Error Handling](#5-error-handling)
  - [6. Connection Success](#6-connection-success)
  - [7. Reconnection Loop](#7-reconnection-loop)
- [Connection Architecture Diagram](#connection-architecture-diagram)
- [Key Features](#key-features)

- [Three-Pump Goroutine Architecture](#three-pump-goroutine-architecture)
- [Detailed Three-Pump Implementation](#detailed-three-pump-implementation)
  - [Entry Point: `Run()` Function](#entry-point-run-function)
  - [Timing Constants](#timing-constants)
  - [Pump 1: `readPump()` - Message Reader](#pump-1-readpump---message-reader)
    - [Key Responsibilities](#key-responsibilities)
    - [Code Flow](#code-flow)
    - [Message Handling Pipeline](#message-handling-pipeline)
  - [Pump 2: `writePump()` - Message Writer](#pump-2-writepump---message-writer)
    - [Key Responsibilities](#key-responsibilities-1)
    - [Code Flow](#code-flow-1)
    - [Send and Wait for Response](#send-and-wait-for-response)
    - [Write Binary Message (with retry)](#write-binary-message-with-retry)
    - [Actual Write Operation](#actual-write-operation)
  - [Pump 3: `heartbeat()` - Ping Sender](#pump-3-heartbeat---ping-sender)
    - [Key Responsibilities](#key-responsibilities-2)
    - [Code Flow](#code-flow-2)
    - [Send Ping Message](#send-ping-message)
    - [Heartbeat Mechanism](#heartbeat-mechanism)
- [Three-Pump Interaction Diagram](#three-pump-interaction-diagram)
- [Key Takeaways](#key-takeaways)

- [Automatic Reconnection Mechanism](#automatic-reconnection-mechanism)
  - [Reconnection Strategy: Exponential Backoff](#reconnection-strategy-exponential-backoff)
    - [Interface Definition](#interface-definition)
    - [Implementation: ExponentialRetry](#implementation-exponentialretry)
    - [Get Sleep Interval](#get-sleep-interval)
    - [Reset Strategy](#reset-strategy)
  - [The `reConn()` Function - Core Reconnection Logic](#the-reconn-function---core-reconnection-logic)
    - [Function Signature](#function-signature)
  - [Step-by-Step Reconnection Flow](#step-by-step-reconnection-flow)
    - [Step 1: Check if Already Connected](#step-1-check-if-already-connected)
    - [Step 2: Lock and Set Connecting State](#step-2-lock-and-set-connecting-state)
    - [Step 3: Build WebSocket URL](#step-3-build-websocket-url)
    - [Step 4: Dial WebSocket Connection](#step-4-dial-websocket-connection)
    - [Step 5: Error Handling - Parse and Classify](#step-5-error-handling---parse-and-classify)
    - [Step 6: Post-Connection Setup (on Success)](#step-6-post-connection-setup-on-success)
  - [Integration with readPump()](#integration-with-readpump)
  - [Reconnection State Diagram](#reconnection-state-diagram)
  - [Reconnection Flow Examples](#reconnection-flow-examples)
    - [Example 1: Temporary Network Failure](#example-1-temporary-network-failure)
    - [Example 2: Token Expiration (Fatal Error)](#example-2-token-expiration-fatal-error)
    - [Example 3: Server Temporarily Down](#example-3-server-temporarily-down)
  - [Reconnection Triggers](#reconnection-triggers)
  - [Key Reconnection Features](#key-reconnection-features)
  - [Thread Safety](#thread-safety)
  - [Configuration](#configuration)

- [Compression & Encoding Pipeline](#compression--encoding-pipeline)
  - [Three-Layer Architecture Overview](#three-layer-architecture-overview)
    - [Layer 1: Protobuf Serialization (Inner Layer)](#layer-1-protobuf-serialization-inner-layer)
    - [Layer 2: GOB Encoding + Metadata Wrapper (Middle Layer)](#layer-2-gob-encoding--metadata-wrapper-middle-layer)
    - [Layer 3: Gzip Compression (Outer Layer)](#layer-3-gzip-compression-outer-layer)
  - [Complete Data Flow](#complete-data-flow)
  - [Message Structures](#message-structures)
    - [Request Message Structure](#request-message-structure)
    - [Response Message Structure](#response-message-structure)
  - [Encoding: GOB Format](#encoding-gob-format)
    - [Encoder Interface](#encoder-interface)
    - [GobEncoder Implementation](#gobencoder-implementation)
    - [Encode Method](#encode-method)
    - [Decode Method](#decode-method)
  - [Compression: Gzip](#compression-gzip)
    - [Compressor Interface](#compressor-interface)
    - [Object Pooling](#object-pooling)
    - [GzipCompressor Implementation](#gzipcompressor-implementation)
    - [Compress With Pool (Preferred)](#compress-with-pool-preferred)
    - [Decompress With Pool (Preferred)](#decompress-with-pool-preferred)
    - [Standard Methods (Without Pool)](#standard-methods-without-pool)
  - [Outbound Message Pipeline](#outbound-message-pipeline)
    - [Complete Send Flow](#complete-send-flow)
  - [Inbound Message Pipeline](#inbound-message-pipeline)
    - [Complete Receive Flow](#complete-receive-flow)
  - [Compression Configuration](#compression-configuration)
    - [Enabling Compression](#enabling-compression)
    - [Setting IsCompression](#setting-iscompression)
  - [Performance Characteristics](#performance-characteristics)
    - [Encoding Performance](#encoding-performance)
    - [Compression Performance](#compression-performance)
    - [Object Pooling Benefits](#object-pooling-benefits)
  - [Error Handling](#error-handling)
    - [Encoding Errors](#encoding-errors)
    - [Compression Errors](#compression-errors)
    - [Decompression Errors](#decompression-errors)
    - [Decoding Errors](#decoding-errors)
  - [Complete Pipeline Example](#complete-pipeline-example)
    - [Sending a Message](#sending-a-message)
    - [Receiving a Message](#receiving-a-message)
  - [Key Takeaways](#key-takeaways-1)

---

## Connection Establishment Flow

### 1. Entry Point: `reConn()` Function

**Location:** `internal/interaction/long_conn_mgr.go`

The `reConn()` function is called from `readPump()` at line 201 in a continuous loop:

```go
func (c *LongConnMgr) reConn(ctx context.Context, num *int) (needRecon bool, err error) {
    // Check if already connected
    if c.IsConnected() {
        return true, nil
    }
    // ... connection logic
}
```

---

### 2. Connection States & Callbacks

**Setting connection state:**

```go
c.listener.OnConnecting()           // Notify app: "connecting..."
c.SetConnectionStatus(Connecting)   // Set status to Connecting
```

**Available connection states** (lines 64-69):

| State | Description |
|-------|-------------|
| `DefaultNotConnect` | Initial state before any connection attempt |
| `Closed` | Connection has been closed |
| `Connecting` | Currently attempting to connect |
| `Connected` | Successfully connected to server |

---

### 3. Build WebSocket URL

**Location:** Lines 637-642

```go
url := fmt.Sprintf("%s?sendID=%s&token=%s&platformID=%d&operationID=%s&isBackground=%t",
    wsAddr, userID, token, platformID, operationID, isBackground)

if c.IsCompression {
    url += fmt.Sprintf("&compression=%s", "gzip")
}
```

**URL parameters:**

| Parameter | Description |
|-----------|-------------|
| `sendID` | User ID |
| `token` | Authentication token |
| `platformID` | Platform identifier (iOS/Android/Web/etc.) |
| `operationID` | Request tracking ID |
| `isBackground` | App state (foreground/background) |
| `compression` | Enable gzip compression (optional) |

---

### 4. Dial WebSocket Connection

**Location:** Line 644

```go
resp, err := c.conn.Dial(url, nil)
```

This establishes the actual WebSocket connection to the server.

---

### 5. Error Handling

**Location:** Lines 645-679

If connection fails, the error type is checked to determine retry strategy:

```go
if err != nil {
    c.SetConnectionStatus(Closed)

    // Parse error response
    var apiResp struct {
        ErrCode int    `json:"errCode"`
        ErrMsg  string `json:"errMsg"`
        ErrDlt  string `json:"errDlt"`
    }
    json.Unmarshal(body, &apiResp)

    // Check if error is fatal (token errors)
    switch apiResp.ErrCode {
    case TokenExpiredError, TokenInvalidError, ...:
        return false, err  // DON'T retry (fatal)
    default:
        return true, err   // DO retry (transient)
    }
}
```

**Fatal errors (no retry):**

- `TokenExpiredError`
- `TokenInvalidError`
- `TokenMalformedError`
- `TokenNotValidYetError`
- `TokenUnknownError`
- `TokenNotExistError`
- `TokenKickedError`

**All other errors:** Retry with exponential backoff

---

### 6. Connection Success

**Location:** Lines 680-698

Upon successful connection, the following actions are performed:

```go
// 1. Write subscription info (for user online status tracking)
c.writeConnFirstSubMsg(ctx)

// 2. Notify success callback
c.listener.OnConnectSuccess()
c.sub.onConnSuccess()

// 3. Update connection state
c.SetConnectionStatus(Connected)

// 4. Set ping/pong handlers for heartbeat
c.conn.SetPongHandler(c.pongHandler)
c.conn.SetPingHandler(c.pingHandler)

// 5. Reset reconnection backoff strategy
c.reconnectStrategy.Reset()

// 6. Trigger message sync
common.TriggerCmdConnected(ctx, c.pushMsgAndMaxSeqCh)
```

---

### 7. Reconnection Loop

**Location:** `readPump()`, lines 201-210

```go
needRecon, err := c.reConn(ctx, &connNum)
if !needRecon {  // Fatal error, stop trying
    c.closedErr = err
    return
}
if err != nil {  // Transient error, retry
    log.ZWarn(c.ctx, "reConn", err)
    time.Sleep(c.reconnectStrategy.GetSleepInterval())  // Exponential backoff
    continue
}
```

---

## Connection Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│              readPump (goroutine)                       │
│                                                         │
│  Loop:                                                  │
│    ┌─────────────────────────────────────────┐         │
│    │  reConn()                               │         │
│    │  ├─ IsConnected? → skip                │         │
│    │  ├─ OnConnecting()                      │         │
│    │  ├─ Build URL with auth params          │         │
│    │  ├─ Dial WebSocket                      │         │
│    │  ├─ Error?                              │         │
│    │  │   ├─ Fatal → return false           │         │
│    │  │   └─ Retry → return true            │         │
│    │  ├─ writeConnFirstSubMsg()              │         │
│    │  ├─ OnConnectSuccess()                  │         │
│    │  ├─ SetPingPongHandlers()               │         │
│    │  └─ TriggerCmdConnected()               │         │
│    └─────────────────────────────────────────┘         │
│           ↓                                             │
│    Sleep with exponential backoff if error              │
│           ↓                                             │
│    Read messages from WebSocket...                      │
└─────────────────────────────────────────────────────────┘
```

---

## Key Features

### 1. **Automatic Reconnection**
Loops with exponential backoff strategy: `1s → 2s → 4s → 8s → 16s`

### 2. **Token Validation**
Stops reconnecting immediately on authentication errors

### 3. **State Management**
Thread-safe connection status tracking with mutexes

### 4. **Heartbeat Setup**
Ping/pong handlers established on successful connection

### 5. **Message Sync**
Triggers message synchronization after connection established

### 6. **Compression**
Optional gzip compression negotiation via URL parameter

---

## Three-Pump Goroutine Architecture

The connection is maintained by **three concurrent goroutines** started in `Run()` at lines 132-136:

| Goroutine | Responsibility |
|-----------|----------------|
| **readPump()** | Reads messages from WebSocket & handles reconnection |
| **writePump()** | Sends queued messages to WebSocket |
| **heartbeat()** | Sends periodic ping frames (every 24 seconds) |

**Reference:** See `internal/interaction/long_conn_mgr.go:132-136`

---

## Detailed Three-Pump Implementation

### Entry Point: `Run()` Function

**Location:** `long_conn_mgr.go:132-136`

```go
func (c *LongConnMgr) Run(ctx context.Context) {
    go c.readPump(ctx)    // Start read goroutine
    go c.writePump(ctx)   // Start write goroutine
    go c.heartbeat(ctx)   // Start heartbeat goroutine
}
```

Each goroutine runs independently and concurrently, communicating through channels.

---

### Timing Constants

**Location:** `long_conn_mgr.go:45-62`

```go
const (
    writeWait            = 10 * time.Second  // Write timeout
    pongWait             = 30 * time.Second  // Expected pong interval
    pingPeriod           = 24 * time.Second  // (pongWait * 8) / 10
    maxMessageSize       = 1024 * 1024       // 1 MB max message size
    maxReconnectAttempts = 300               // Max reconnection attempts
    sendAndWaitTime      = 10 * time.Second  // Response timeout
)
```

---

### Pump 1: `readPump()` - Message Reader

**Location:** `long_conn_mgr.go:177-236`

**Purpose:** Reads incoming messages from WebSocket, handles reconnection, and routes messages.

#### Key Responsibilities

1. **Panic Recovery** - Captures and logs any panics
2. **Context Cancellation** - Respects logout/shutdown signals
3. **Automatic Reconnection** - Calls `reConn()` on connection loss
4. **Message Reading** - Reads binary frames from WebSocket
5. **Message Routing** - Routes messages to appropriate handlers

#### Code Flow

```go
func (c *LongConnMgr) readPump(ctx context.Context) {
    // 1. Setup panic recovery
    defer func() {
        if r := recover(); r != nil {
            err := fmt.Sprintf("panic: %+v\n%s", r, debug.Stack())
            log.ZWarn(ctx, "readPump panic", nil, "panic info", err)
        }
    }()

    log.ZDebug(ctx, "readPump start", "goroutine ID:", getGoroutineID())

    // 2. Cleanup on exit
    defer func() {
        _ = c.close()
        log.ZWarn(c.ctx, "readPump closed", c.closedErr)
    }()

    connNum := 0
    for {
        // 3. Check context cancellation (logout)
        select {
        case <-ctx.Done():
            c.closedErr = ctx.Err()
            log.ZInfo(c.ctx, "readPump done, sdk logout.....")
            return
        default:
        }

        // 4. Generate new operation ID for each iteration
        ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())

        // 5. Reconnect if needed
        needRecon, err := c.reConn(ctx, &connNum)
        if !needRecon {  // Fatal error (e.g., token expired)
            c.closedErr = err
            return
        }
        if err != nil {  // Transient error, retry
            log.ZWarn(c.ctx, "reConn", err)
            time.Sleep(c.reconnectStrategy.GetSleepInterval())
            continue
        }

        // 6. Configure connection
        c.conn.SetReadLimit(maxMessageSize)
        _ = c.conn.SetReadDeadline(pongWait)

        // 7. Read message from WebSocket
        messageType, message, err := c.conn.ReadMessage()
        if err != nil {
            log.ZError(c.ctx, "readMessage err", err, "goroutine ID:", getGoroutineID())
            _ = c.close()
            c.sub.onConnClosed(err)
            continue
        }

        // 8. Process message based on type
        switch messageType {
        case MessageBinary:
            err := c.handleMessage(message)  // Decompress → Decode → Route
            if err != nil {
                c.closedErr = err
                return
            }
        case MessageText:
            c.closedErr = ErrNotSupportMessageProtocol
            return
        case CloseMessage:
            c.closedErr = ErrClientClosed
            return
        default:
        }
    }
}
```

#### Message Handling Pipeline

**Location:** `long_conn_mgr.go:467-528`

```go
func (c *LongConnMgr) handleMessage(message []byte) error {
    // 1. Decompress (if compression enabled)
    if c.IsCompression {
        message, decompressErr = c.compressor.DecompressWithPool(message)
        if decompressErr != nil {
            return sdkerrs.ErrMsgDeCompression
        }
    }

    // 2. Decode from GOB format
    var wsResp GeneralWsResp
    err := c.encoder.Decode(message, &wsResp)
    if err != nil {
        return sdkerrs.ErrMsgDecodeBinaryWs
    }

    // 3. Route by request identifier
    switch wsResp.ReqIdentifier {
    case constant.PushMsg:
        // New message push from server
        c.doPushMsg(ctx, wsResp)

    case constant.LogoutMsg:
        // Server-initiated logout
        c.Syncer.NotifyResp(ctx, wsResp)
        return sdkerrs.ErrLoginOut

    case constant.KickOnlineMsg:
        // User kicked offline
        err = errs.ErrTokenKicked.WrapMsg("client kicked offline")
        ccontext.GetApiErrCodeCallback(ctx).OnError(ctx, err)
        return err

    case constant.SendMsg, constant.GetNewestSeq, constant.PullMsgByRange:
        // Response to client request
        c.Syncer.NotifyResp(ctx, wsResp)

    case constant.WsSubUserOnlineStatus:
        // User online status change
        c.handlerUserOnlineChange(ctx, wsResp)

    default:
        return sdkerrs.ErrMsgBinaryTypeNotSupport
    }
    return nil
}
```

**Message Flow:**
```
Binary WebSocket Frame
    ↓
gzip Decompress (if enabled)
    ↓
GOB Decode → GeneralWsResp struct
    ↓
Route by ReqIdentifier
    ↓
    ├─ PushMsg → doPushMsg() → Message sync
    ├─ LogoutMsg → Trigger logout
    ├─ KickOnlineMsg → Callback OnKickedOffline
    ├─ SendMsg/GetNewestSeq → NotifyResp() → Unblock waiting channel
    └─ WsSubUserOnlineStatus → Update user online status
```

---

### Pump 2: `writePump()` - Message Writer

**Location:** `long_conn_mgr.go:243-298`

**Purpose:** Sends outbound messages from the `send` channel to WebSocket.

#### Key Responsibilities

1. **Panic Recovery** - Captures and logs any panics
2. **Context Cancellation** - Respects logout/shutdown signals
3. **Channel Reading** - Reads messages from `c.send` channel
4. **Message Sending** - Writes messages to WebSocket with retry
5. **Response Notification** - Notifies waiting goroutines of responses

#### Code Flow

```go
func (c *LongConnMgr) writePump(ctx context.Context) {
    // 1. Setup panic recovery
    defer func() {
        if r := recover(); r != nil {
            err := fmt.Sprintf("panic: %+v\n%s", r, debug.Stack())
            log.ZWarn(ctx, "writePump panic", nil, "panic info", err)
        }
    }()

    log.ZDebug(ctx, "writePump start", "goroutine ID:", getGoroutineID())

    // 2. Cleanup: close connection and send channel
    defer func() {
        c.close()
        close(c.send)
    }()

    for {
        select {
        // 3. Check context cancellation (logout)
        case <-ctx.Done():
            c.closedErr = ctx.Err()
            log.ZInfo(c.ctx, "writePump done, sdk logout.....")
            return

        // 4. Receive message from send channel
        case message, ok := <-c.send:
            if !ok {
                // Channel closed, send close frame
                _ = c.conn.SetWriteDeadline(writeWait)
                err := c.conn.WriteMessage(websocket.CloseMessage, []byte{})
                if err != nil {
                    log.ZError(c.ctx, "send close message error", err)
                }
                c.closedErr = ErrChanClosed
                return
            }

            log.ZDebug(c.ctx, "writePump recv message",
                "reqIdentifier", message.Message.ReqIdentifier,
                "operationID", message.Message.OperationID,
                "sendID", message.Message.SendID)

            // 5. Send message and wait for response
            resp, err := c.sendAndWaitResp(&message.Message)
            if err != nil {
                // Create error response
                resp = &GeneralWsResp{
                    ReqIdentifier: message.Message.ReqIdentifier,
                    OperationID:   message.Message.OperationID,
                    Data:          nil,
                }
                if code, ok := errs.Unwrap(err).(errs.CodeError); ok {
                    resp.ErrCode = code.Code()
                    resp.ErrMsg = code.Msg()
                } else {
                    log.ZError(c.ctx, "writeBinaryMsgAndRetry failed", err, "wsReq", message.Message)
                }
            }

            // 6. Notify waiting channel with response
            nErr := c.Syncer.notifyCh(message.Resp, resp, 1)
            if nErr != nil {
                log.ZError(c.ctx, "TriggerCmdNewMsgCome failed", nErr, "wsResp", resp)
            }
        }
    }
}
```

#### Send and Wait for Response

**Location:** `long_conn_mgr.go:356-370`

```go
func (c *LongConnMgr) sendAndWaitResp(msg *GeneralWsReq) (*GeneralWsResp, error) {
    // 1. Write message to WebSocket (with retry)
    tempChan, err := c.writeBinaryMsgAndRetry(msg)
    defer c.Syncer.DelCh(msg.MsgIncr)  // Cleanup channel mapping

    if err != nil {
        return nil, err
    }

    // 2. Wait for response (with timeout)
    select {
    case resp := <-tempChan:
        return resp, nil  // Response received
    case <-time.After(sendAndWaitTime):  // 10 seconds timeout
        return nil, sdkerrs.ErrNetworkTimeOut
    }
}
```

#### Write Binary Message (with retry)

**Location:** `long_conn_mgr.go:372-391`

```go
func (c *LongConnMgr) writeBinaryMsgAndRetry(msg *GeneralWsReq) (chan *GeneralWsResp, error) {
    // 1. Add channel for async response tracking
    msgIncr, tempChan := c.Syncer.AddCh(msg.SendID)
    msg.MsgIncr = msgIncr  // Unique message ID

    // 2. Retry up to maxReconnectAttempts (300 times)
    for i := 0; i < maxReconnectAttempts; i++ {
        err := c.writeBinaryMsg(*msg)  // Encode → Compress → Write
        if err != nil {
            log.ZError(c.ctx, "send binary message error", err, "message", msg)
            c.closedErr = err
            _ = c.close()
            time.Sleep(time.Second * 1)  // Wait 1 second before retry
            continue
        } else {
            return tempChan, nil  // Success
        }
    }
    return nil, sdkerrs.ErrNetwork.WrapMsg("send binary message error")
}
```

#### Actual Write Operation

**Location:** `long_conn_mgr.go:436-454`

```go
func (c *LongConnMgr) writeBinaryMsgNoLock(req GeneralWsReq) error {
    // 1. Encode to GOB format
    encodeBuf, err := c.encoder.Encode(req)
    if err != nil {
        return err
    }

    // 2. Check connection status
    if c.GetConnectionStatus() != Connected {
        return sdkerrs.ErrNetwork.WrapMsg("connection closed,re conning...")
    }

    // 3. Set write deadline
    _ = c.conn.SetWriteDeadline(writeWait)

    // 4. Compress if enabled, then write
    if c.IsCompression {
        resultBuf, compressErr := c.compressor.CompressWithPool(encodeBuf)
        if compressErr != nil {
            return compressErr
        }
        return c.conn.WriteMessage(MessageBinary, resultBuf)
    } else {
        return c.conn.WriteMessage(MessageBinary, encodeBuf)
    }
}
```

**Message Flow:**
```
Client calls SendReqWaitResp()
    ↓
Create Message with Resp channel
    ↓
Send to c.send channel → writePump receives
    ↓
writePump: sendAndWaitResp()
    ├─ writeBinaryMsgAndRetry()
    │   ├─ Add channel tracking (msgIncr)
    │   └─ writeBinaryMsg()
    │       ├─ GOB Encode
    │       ├─ gzip Compress (if enabled)
    │       └─ WebSocket WriteMessage()
    └─ Wait for response (10s timeout)
        ↓
readPump receives response
    ↓
handleMessage() → Syncer.NotifyResp()
    ↓
Response sent to tempChan
    ↓
sendAndWaitResp() returns response
    ↓
Notify message.Resp channel
    ↓
Original caller receives response
```

---

### Pump 3: `heartbeat()` - Ping Sender

**Location:** `long_conn_mgr.go:300-326`

**Purpose:** Sends periodic ping frames to keep connection alive and detect dead connections.

#### Key Responsibilities

1. **Panic Recovery** - Captures and logs any panics
2. **Context Cancellation** - Respects logout/shutdown signals
3. **Periodic Pinging** - Sends ping every 24 seconds
4. **Connection Health** - Detects dead connections via pong timeout

#### Code Flow

```go
func (c *LongConnMgr) heartbeat(ctx context.Context) {
    // 1. Setup panic recovery
    defer func() {
        if r := recover(); r != nil {
            err := fmt.Sprintf("panic: %+v\n%s", r, debug.Stack())
            log.ZWarn(ctx, "heartbeat panic", nil, "panic info", err)
        }
    }()

    log.ZDebug(ctx, "heartbeat start", "goroutine ID:", getGoroutineID())

    // 2. Create ticker (fires every pingPeriod = 24 seconds)
    ticker := time.NewTicker(pingPeriod)
    defer func() {
        ticker.Stop()
        log.ZWarn(c.ctx, "heartbeat closed", nil, "heartbeat", "heartbeat done sdk logout.....")
    }()

    for {
        select {
        // 3. Check context cancellation (logout)
        case <-ctx.Done():
            log.ZInfo(ctx, "heartbeat done sdk logout.....")
            return

        // 4. Send ping every 24 seconds
        case <-ticker.C:
            log.ZInfo(ctx, "sendPingMessage", "goroutine ID:", getGoroutineID())
            c.sendPingMessage(ctx)
        }
    }
}
```

#### Send Ping Message

**Location:** `long_conn_mgr.go:328-343`

```go
func (c *LongConnMgr) sendPingMessage(ctx context.Context) {
    c.connWrite.Lock()  // Ensure only one write at a time
    defer c.connWrite.Unlock()

    opid := utils.OperationIDGenerator()
    log.ZDebug(ctx, "ping Message Started", "goroutine ID:", getGoroutineID(), "opid", opid)

    if c.IsConnected() {
        log.ZDebug(ctx, "ping Message Started isConnected", "goroutine ID:", getGoroutineID(), "opid", opid)

        // Set write deadline (10 seconds)
        c.conn.SetWriteDeadline(writeWait)

        // Send ping frame with operation ID as payload
        if err := c.conn.WriteMessage(PingMessage, []byte(opid)); err != nil {
            log.ZWarn(ctx, "ping Message failed", err, "goroutine ID:", getGoroutineID(), "opid", opid)
            return
        }
    } else {
        log.ZDebug(ctx, "ping Message failed, connection", "connStatus", c.GetConnectionStatus(), "goroutine ID:", getGoroutineID(), "opid", opid)
    }
}
```

#### Heartbeat Mechanism

**Timing:**
- **Ping sent:** Every 24 seconds (pingPeriod)
- **Pong expected:** Within 30 seconds (pongWait)
- **Write timeout:** 10 seconds (writeWait)

**Flow:**
```
heartbeat() ticker fires (every 24s)
    ↓
sendPingMessage()
    ├─ Lock write mutex
    ├─ Check if connected
    ├─ Set write deadline (10s)
    └─ WriteMessage(PingMessage)
        ↓
Server receives ping
    ↓
Server sends pong
    ↓
readPump() receives pong (via pongHandler)
    ↓
pongHandler() → SetReadDeadline(30s)
    ↓
If no pong received in 30s:
    ├─ Read deadline expires
    ├─ ReadMessage() returns error
    ├─ Connection closed
    └─ Reconnection triggered
```

---

## Three-Pump Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          LongConnMgr                            │
│                                                                 │
│   ┌──────────────┐      ┌──────────────┐      ┌─────────────┐ │
│   │  readPump()  │      │ writePump()  │      │ heartbeat() │ │
│   │ (goroutine)  │      │ (goroutine)  │      │(goroutine)  │ │
│   └──────┬───────┘      └──────┬───────┘      └──────┬──────┘ │
│          │                     │                      │        │
│          │                     │                      │        │
└──────────┼─────────────────────┼──────────────────────┼────────┘
           │                     │                      │
           │                     │                      │
     ┌─────▼─────┐         ┌─────▼─────┐         ┌─────▼─────┐
     │  WebSocket│         │  c.send   │         │  Ticker   │
     │ReadMessage│         │  channel  │         │  (24s)    │
     └─────┬─────┘         └─────┬─────┘         └─────┬─────┘
           │                     │                      │
           │ Binary frame        │ Message              │ Time event
           │                     │                      │
     ┌─────▼─────────┐     ┌─────▼─────────┐     ┌─────▼─────────┐
     │ Decompress    │     │ Add msgIncr   │     │ sendPing()    │
     │ GOB Decode    │     │ GOB Encode    │     │ WriteMessage  │
     │               │     │ Compress      │     │ (PingMessage) │
     └─────┬─────────┘     │ WriteMessage  │     └───────────────┘
           │               └─────┬─────────┘
           │                     │
     ┌─────▼─────────┐     ┌─────▼─────────┐
     │ Route by      │     │ Wait response │
     │ ReqIdentifier │     │ (10s timeout) │
     └─────┬─────────┘     └─────┬─────────┘
           │                     │
     ┌─────▼─────────┐     ┌─────▼─────────┐
     │ - PushMsg     │     │ Notify channel│
     │ - LogoutMsg   │     │ Response      │
     │ - SendMsg     │     │ received      │
     │ - KickMsg     │     └───────────────┘
     └───────────────┘
```

---

## Key Takeaways

### 1. **Separation of Concerns**
Each pump handles one responsibility:
- **readPump** - Reading and routing
- **writePump** - Sending and response tracking
- **heartbeat** - Connection health monitoring

### 2. **Thread-Safe Communication**
- Pumps communicate via **channels** (not shared memory)
- Write operations protected by **mutex** (`c.connWrite`)
- Connection status protected by **mutex** (`c.w`)

### 3. **Graceful Shutdown**
All pumps respect context cancellation:
```go
select {
case <-ctx.Done():
    return  // Clean exit
}
```

### 4. **Automatic Recovery**
- **Panic recovery** in all pumps (defer + recover)
- **Automatic reconnection** in readPump
- **Retry logic** in writePump (up to 300 attempts)

### 5. **Request-Response Pattern**
Uses async channels for request-response tracking:
```go
msgIncr → tempChan mapping (WsRespAsyn)
Client blocks on tempChan
readPump notifies tempChan when response arrives
```

### 6. **Timeout Management**
- **Write timeout:** 10 seconds
- **Pong timeout:** 30 seconds
- **Response timeout:** 10 seconds
- **Read deadline:** Updated on pong receipt

---

## Automatic Reconnection Mechanism

The SDK implements a robust automatic reconnection system that handles connection failures gracefully and attempts to restore connectivity using an exponential backoff strategy.

---

### Reconnection Strategy: Exponential Backoff

**Location:** `internal/interaction/reconnect.go`

#### Interface Definition

```go
type ReconnectStrategy interface {
    GetSleepInterval() time.Duration  // Returns next sleep duration
    Reset()                           // Resets strategy to initial state
}
```

#### Implementation: ExponentialRetry

```go
type ExponentialRetry struct {
    attempts []int  // Backoff intervals: [1, 2, 4, 8, 16] seconds
    index    int    // Current position in attempts array
}

func NewExponentialRetry() *ExponentialRetry {
    return &ExponentialRetry{
        attempts: []int{1, 2, 4, 8, 16},
        index:    -1,  // Start at -1, first increment makes it 0
    }
}
```

#### Get Sleep Interval

```go
func (rs *ExponentialRetry) GetSleepInterval() time.Duration {
    rs.index++  // Increment index
    interval := rs.index % len(rs.attempts)  // Cycle through attempts
    return time.Second * time.Duration(rs.attempts[interval])
}
```

**Backoff Sequence:**
```
Attempt 1: 1 second   (index 0)
Attempt 2: 2 seconds  (index 1)
Attempt 3: 4 seconds  (index 2)
Attempt 4: 8 seconds  (index 3)
Attempt 5: 16 seconds (index 4)
Attempt 6: 1 second   (index 5 % 5 = 0, cycles back)
Attempt 7: 2 seconds  (index 6 % 5 = 1)
... and so on
```

#### Reset Strategy

```go
func (rs *ExponentialRetry) Reset() {
    rs.index = -1  // Reset to initial state
}
```

Called when connection is successfully established, resetting backoff for future failures.

---

### The `reConn()` Function - Core Reconnection Logic

**Location:** `long_conn_mgr.go:629-699`

This function is called from `readPump()` in every iteration of the read loop.

#### Function Signature

```go
func (c *LongConnMgr) reConn(ctx context.Context, num *int) (needRecon bool, err error)
```

**Parameters:**
- `ctx` - Context for cancellation and metadata
- `num` - Connection counter (tracks number of successful connections)

**Return Values:**
- `needRecon` - `true` if should retry, `false` if fatal error (stop reconnection)
- `err` - Error if connection failed, `nil` if successful

---

### Step-by-Step Reconnection Flow

#### Step 1: Check if Already Connected

```go
if c.IsConnected() {
    return true, nil  // Already connected, no need to reconnect
}
```

Quick check to avoid unnecessary reconnection attempts.

#### Step 2: Lock and Set Connecting State

```go
c.connWrite.Lock()  // Prevent concurrent writes during reconnection
defer c.connWrite.Unlock()

c.listener.OnConnecting()          // Notify app: "connecting..."
c.SetConnectionStatus(Connecting)  // Update internal state
```

**State Management:**
- Locks write operations to ensure thread safety
- Triggers `OnConnecting()` callback to update UI
- Sets connection status to `Connecting`

#### Step 3: Build WebSocket URL

```go
url := fmt.Sprintf("%s?sendID=%s&token=%s&platformID=%d&operationID=%s&isBackground=%t",
    ccontext.Info(ctx).WsAddr(),      // WebSocket server address
    ccontext.Info(ctx).UserID(),      // User ID
    ccontext.Info(ctx).Token(),       // Authentication token
    ccontext.Info(ctx).PlatformID(),  // Platform (iOS/Android/Web/etc.)
    ccontext.Info(ctx).OperationID(), // Request tracking ID
    c.GetBackground())                // App foreground/background state

if c.IsCompression {
    url += fmt.Sprintf("&compression=%s", "gzip")
}

log.ZDebug(ctx, "conn start", "url", url)
```

**URL Parameters:**
- `sendID` - Identifies the user
- `token` - JWT or session token for authentication
- `platformID` - Platform identifier (1=iOS, 2=Android, 3=Windows, etc.)
- `operationID` - Unique ID for request tracing
- `isBackground` - Whether app is in background (affects server behavior)
- `compression` - Optional gzip compression

#### Step 4: Dial WebSocket Connection

```go
resp, err := c.conn.Dial(url, nil)
```

Attempts to establish WebSocket connection with the server.

#### Step 5: Error Handling - Parse and Classify

```go
if err != nil {
    c.SetConnectionStatus(Closed)  // Mark connection as closed

    if resp != nil {
        // Read error response body
        body, err := io.ReadAll(resp.Body)
        if err != nil {
            return true, err  // Network error, retry
        }

        log.ZInfo(ctx, "reConn resp", "body", string(body))

        // Parse error response
        var apiResp struct {
            ErrCode int    `json:"errCode"`
            ErrMsg  string `json:"errMsg"`
            ErrDlt  string `json:"errDlt"`
        }
        if err := json.Unmarshal(body, &apiResp); err != nil {
            return true, err  // Parse error, retry
        }

        // Create structured error
        err = errs.NewCodeError(apiResp.ErrCode, apiResp.ErrMsg).
            WithDetail(apiResp.ErrDlt).Wrap()

        // Trigger error callback
        ccontext.GetApiErrCodeCallback(ctx).OnError(ctx, err)

        // Classify error as fatal or transient
        switch apiResp.ErrCode {
        case
            errs.TokenExpiredError,      // Token has expired
            errs.TokenInvalidError,      // Token is invalid
            errs.TokenMalformedError,    // Token format is wrong
            errs.TokenNotValidYetError,  // Token not yet valid
            errs.TokenUnknownError,      // Token unknown
            errs.TokenNotExistError,     // Token doesn't exist
            errs.TokenKickedError:       // User kicked offline
            return false, err  // FATAL: Don't retry
        default:
            return true, err   // TRANSIENT: Retry with backoff
        }
    }

    // Network error or no response
    c.listener.OnConnectFailed(sdkerrs.NetworkError, err.Error())
    return true, err  // Retry
}
```

**Error Classification:**

| Error Type | Action | Reason |
|------------|--------|--------|
| **Fatal Errors** | Stop reconnection (`needRecon = false`) | Auth issues that won't resolve with retry |
| - TokenExpiredError | Stop | Token needs refresh from app |
| - TokenInvalidError | Stop | Token is invalid, can't authenticate |
| - TokenKickedError | Stop | User kicked by another login |
| **Transient Errors** | Retry with backoff (`needRecon = true`) | Temporary issues that may resolve |
| - Network errors | Retry | Connection may be restored |
| - Server errors (5xx) | Retry | Server may recover |
| - Timeout errors | Retry | May succeed next time |

#### Step 6: Post-Connection Setup (on Success)

```go
// 1. Write initial subscription info
if err := c.writeConnFirstSubMsg(ctx); err != nil {
    log.ZError(ctx, "first write user online sub info error", err)
    ccontext.GetApiErrCodeCallback(ctx).OnError(ctx, err)
    c.listener.OnConnectFailed(sdkerrs.NetworkError, err.Error())
    c.conn.Close()
    return true, err  // Failed initial write, retry
}
```

**writeConnFirstSubMsg()** - Sends user online status subscriptions to server.

**Location:** `long_conn_mgr.go:575-586`

```go
func (c *LongConnMgr) writeConnFirstSubMsg(ctx context.Context) error {
    // Get list of user IDs to subscribe for online status
    userIDs := c.sub.getNewConnSubUserIDs()
    log.ZDebug(ctx, "writeConnFirstSubMsg getNewConnSubUserIDs", "userIDs", userIDs)

    if len(userIDs) == 0 {
        return nil  // No subscriptions needed
    }

    // Send subscription request
    if err := c.writeSubInfo(userIDs, nil, false); err != nil {
        c.sub.onConnClosed(err)
        return err
    }

    return nil
}
```

This ensures the server knows which users' online status the client wants to track.

```go
// 2. Trigger success callbacks
c.listener.OnConnectSuccess()  // Notify app: "connected"
c.sub.onConnSuccess()           // Notify subscription manager

// 3. Store connection context
c.ctx = newContext(c.conn.LocalAddr())
c.ctx = context.WithValue(ctx, "ConnContext", c.ctx)

// 4. Update connection state
c.SetConnectionStatus(Connected)

// 5. Setup ping/pong handlers
c.conn.SetPongHandler(c.pongHandler)
c.conn.SetPingHandler(c.pingHandler)

// 6. Increment connection counter
*num++
log.ZInfo(c.ctx, "long conn establish success", "localAddr", c.conn.LocalAddr(), "connNum", *num)

// 7. Reset reconnection strategy
c.reconnectStrategy.Reset()

// 8. Trigger message sync
_ = common.TriggerCmdConnected(ctx, c.pushMsgAndMaxSeqCh)

return true, nil  // Connection successful
```

**Post-Connection Actions:**

| Step | Action | Purpose |
|------|--------|---------|
| 1 | `writeConnFirstSubMsg()` | Subscribe to user online status |
| 2 | `OnConnectSuccess()` | Notify app UI connection restored |
| 3 | Store context | Track connection metadata |
| 4 | `SetConnectionStatus(Connected)` | Update internal state |
| 5 | Setup ping/pong handlers | Enable heartbeat mechanism |
| 6 | Increment counter | Track connection instances |
| 7 | `Reset()` | Reset backoff for future failures |
| 8 | Trigger message sync | Start syncing missed messages |

---

### Integration with readPump()

**Location:** `long_conn_mgr.go:201-210`

The reconnection logic is integrated into the main read loop:

```go
for {
    // Check context cancellation
    select {
    case <-ctx.Done():
        c.closedErr = ctx.Err()
        log.ZInfo(c.ctx, "readPump done, sdk logout.....")
        return
    default:
    }

    // Generate new operation ID for tracing
    ctx = ccontext.WithOperationID(ctx, utils.OperationIDGenerator())

    // RECONNECTION LOGIC
    needRecon, err := c.reConn(ctx, &connNum)
    if !needRecon {
        // Fatal error (e.g., token expired)
        // Stop reconnection loop
        c.closedErr = err
        return
    }
    if err != nil {
        // Transient error, retry with backoff
        log.ZWarn(c.ctx, "reConn", err)
        time.Sleep(c.reconnectStrategy.GetSleepInterval())  // Exponential backoff
        continue  // Retry connection
    }

    // Connection successful, proceed to read messages
    c.conn.SetReadLimit(maxMessageSize)
    _ = c.conn.SetReadDeadline(pongWait)

    // Read message...
    messageType, message, err := c.conn.ReadMessage()
    // ... process message
}
```

---

### Reconnection State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    readPump() Loop                          │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │ reConn()    │
                   └──────┬──────┘
                          │
                   ┌──────▼─────────┐
                   │ IsConnected()? │
                   └──────┬─────────┘
                          │
            ┌─────────────┼─────────────┐
            │ Yes                       │ No
            ▼                           ▼
    ┌──────────────┐          ┌─────────────────┐
    │ return true  │          │ OnConnecting()  │
    └──────────────┘          │ Dial(url)       │
                              └────────┬─────────┘
                                       │
                         ┌─────────────┼─────────────┐
                         │ Success                    │ Error
                         ▼                            ▼
              ┌────────────────────┐      ┌──────────────────────┐
              │ writeConnFirstSub  │      │ Parse error response │
              │ OnConnectSuccess() │      └─────────┬────────────┘
              │ SetPongHandler()   │                │
              │ Reset()            │      ┌─────────▼────────────┐
              │ TriggerSync()      │      │ Classify error type  │
              │ return true, nil   │      └─────────┬────────────┘
              └────────────────────┘                │
                       │                   ┌────────┼────────┐
                       │                   │ Fatal           │ Transient
                       │                   ▼                 ▼
                       │         ┌──────────────┐  ┌──────────────────┐
                       │         │ return false │  │ return true, err │
                       │         └──────────────┘  └─────────┬────────┘
                       │                 │                    │
                       ▼                 ▼                    ▼
              ┌─────────────┐   ┌──────────────┐   ┌─────────────────┐
              │ Read msgs   │   │ Stop loop    │   │ Sleep(backoff)  │
              │ from socket │   │ Exit pump    │   │ Continue loop   │
              └─────────────┘   └──────────────┘   └─────────────────┘
```

---

### Reconnection Flow Examples

#### Example 1: Temporary Network Failure

```
User disconnects WiFi
    ↓
ReadMessage() returns error
    ↓
close() called → Connection closed
    ↓
Next loop iteration: reConn()
    ↓
Dial() fails (no network)
    ↓
OnConnectFailed() callback
    ↓
return true, err (retry)
    ↓
Sleep 1 second (first attempt)
    ↓
Next loop iteration: reConn()
    ↓
Dial() fails (still no network)
    ↓
return true, err (retry)
    ↓
Sleep 2 seconds (second attempt)
    ↓
User reconnects WiFi
    ↓
Next loop iteration: reConn()
    ↓
Dial() succeeds!
    ↓
writeConnFirstSubMsg()
    ↓
OnConnectSuccess() callback
    ↓
Reset() → backoff resets to 1s
    ↓
TriggerSync() → Sync missed messages
    ↓
Read messages normally
```

#### Example 2: Token Expiration (Fatal Error)

```
Connection drops
    ↓
Next loop iteration: reConn()
    ↓
Dial() fails with TokenExpiredError
    ↓
Parse error response → ErrCode = TokenExpiredError
    ↓
OnError() callback (app should refresh token)
    ↓
return false, err (DON'T retry)
    ↓
readPump() exits
    ↓
writePump() and heartbeat() exit (context cancelled)
    ↓
App must call Login() again with new token
```

#### Example 3: Server Temporarily Down

```
Server crashes
    ↓
ReadMessage() returns error
    ↓
close() called
    ↓
reConn(): Dial() fails (connection refused)
    ↓
return true, err (retry)
    ↓
Sleep sequence: 1s → 2s → 4s → 8s → 16s → 1s → 2s → ...
    ↓
Server comes back online after 30 seconds
    ↓
reConn(): Dial() succeeds
    ↓
Connection restored
    ↓
TriggerSync() → Sync all missed messages
```

---

### Reconnection Triggers

The reconnection mechanism is triggered in several scenarios:

| Trigger | Location | Reason |
|---------|----------|--------|
| **Read Error** | `readPump():213-218` | WebSocket read fails (network issue, server disconnect) |
| **Write Error** | `writeBinaryMsg():441-442` | Can't write to socket (connection lost) |
| **Heartbeat Timeout** | `readPump():212` | No pong received in 30s (dead connection) |
| **Manual Close** | `Close():709-721` | App calls `NetworkStatusChanged()` |

**Read Error Trigger:**
```go
messageType, message, err := c.conn.ReadMessage()
if err != nil {
    log.ZError(c.ctx, "readMessage err", err, "goroutine ID:", getGoroutineID())
    _ = c.close()  // Close current connection
    c.sub.onConnClosed(err)
    continue  // Next loop iteration → reConn() called
}
```

**Heartbeat Timeout Trigger:**
```go
// Set read deadline (30 seconds)
_ = c.conn.SetReadDeadline(pongWait)

// If no data received in 30s, ReadMessage() times out
messageType, message, err := c.conn.ReadMessage()
// → err will be timeout error → connection closed → reconnection
```

---

### Key Reconnection Features

#### 1. **Non-Blocking**
Reconnection happens in the background without blocking other operations.

#### 2. **Automatic and Transparent**
App doesn't need to manually trigger reconnection - happens automatically.

#### 3. **Smart Error Handling**
Distinguishes between fatal errors (stop) and transient errors (retry).

#### 4. **Exponential Backoff**
Prevents overwhelming server with rapid reconnection attempts.

#### 5. **State Callbacks**
App is notified of connection state changes:
- `OnConnecting()` - Attempting to connect
- `OnConnectSuccess()` - Successfully connected
- `OnConnectFailed(code, msg)` - Connection attempt failed

#### 6. **Message Sync Integration**
Automatically triggers message sync after reconnection to fetch missed messages.

#### 7. **Unlimited Attempts**
Continues trying until either:
- Connection succeeds
- Fatal error occurs
- User logs out (`ctx.Done()`)

---

### Thread Safety

Reconnection logic is thread-safe:

```go
// Write lock prevents concurrent connection attempts
c.connWrite.Lock()
defer c.connWrite.Unlock()

// Connection status mutex
c.w.Lock()
defer c.w.Unlock()
c.connStatus = Connecting
```

Only one goroutine (readPump) handles reconnection, preventing race conditions.

---

### Configuration

Reconnection behavior is configured by constants:

```go
const (
    pongWait             = 30 * time.Second  // Connection timeout
    maxReconnectAttempts = 300               // Max attempts per message send
)

// Backoff sequence
attempts: []int{1, 2, 4, 8, 16}  // In seconds
```

**Customization:**
To modify reconnection behavior, adjust:
- Backoff intervals in `NewExponentialRetry()`
- Timeout values in constants
- Fatal error codes in error classification switch statement

---

## Compression & Encoding Pipeline

The SDK uses a **three-layer encoding architecture** for message transmission. Each layer serves a specific purpose in the data serialization and transmission pipeline.

---

### Three-Layer Architecture Overview

**Data is wrapped in multiple layers like an onion:**

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 3: Gzip Compression (Outer Layer)                   │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Layer 2: GOB Encoding (Middle Layer)                 │ │
│  │  ┌─────────────────────────────────────────────────┐  │ │
│  │  │  Layer 1: Protobuf Serialization (Inner Layer) │  │ │
│  │  │                                                  │  │ │
│  │  │  Business Data (Message content, metadata, etc.)│  │ │
│  │  └─────────────────────────────────────────────────┘  │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

#### Layer 1: Protobuf Serialization (Inner Layer)

**Purpose:** Serialize business data into a compact binary format

**Process:**
```go
// Application data in Go struct
textMsg := &sdkws.MsgData{
    Content:     []byte("Hello, world!"),
    SendID:      "user123",
    RecvID:      "user456",
    ContentType: 101,  // Text message
    // ... other fields
}

// Serialize to protobuf bytes
protoData, _ := proto.Marshal(textMsg)  // ~200 bytes
```

**Why Protobuf?**
- **Cross-platform**: Works across Go, Java, Swift, JavaScript, etc.
- **Compact**: Binary format smaller than JSON
- **Schema-based**: Type-safe with defined message structures
- **Fast**: Efficient serialization/deserialization

#### Layer 2: GOB Encoding + Metadata Wrapper (Middle Layer)

**Purpose:** Wrap protobuf data with routing and tracing metadata, then serialize the entire structure

**Process:**
```go
// Wrap protobuf data with metadata
req := GeneralWsReq{
    ReqIdentifier: constant.SendMsg,     // ← Metadata: message type
    Token:         "auth_token",         // ← Metadata: authentication
    SendID:        "user123",            // ← Metadata: sender ID
    OperationID:   "op_abc123",          // ← Metadata: tracing ID
    MsgIncr:       "msg_unique_id",      // ← Metadata: message ID
    Data:          protoData,            // ← Inner layer: protobuf bytes
}

// GOB encodes the ENTIRE struct (metadata + protobuf data together)
encoder := NewGobEncoder()
gobBytes, _ := encoder.Encode(req)  // ~220 bytes (includes struct overhead)
```

**What's in the wrapper?**
- **Routing metadata**: `ReqIdentifier` tells server how to process this message
- **Authentication**: `Token` for authorization
- **Tracing**: `OperationID` for distributed tracing across services
- **Request-Response matching**: `MsgIncr` to match responses to requests
- **User context**: `SendID` identifies the sender
- **Actual data**: `Data` field contains the protobuf bytes

**Why GOB?**
- **Native Go format**: Easy to use with Go structs
- **Automatic**: Handles complex nested structures
- **Type preservation**: Maintains Go types during serialization

#### Layer 3: Gzip Compression (Outer Layer)

**Purpose:** Compress the entire GOB-encoded datagram for network efficiency

**Process:**
```go
// Compress the entire GOB-encoded datagram
compressor := NewGzipCompressor()
compressed, _ := compressor.CompressWithPool(gobBytes)  // ~60 bytes (73% reduction!)

// Send compressed bytes over WebSocket
conn.WriteMessage(MessageBinary, compressed)
```

**Why Gzip?**
- **Bandwidth savings**: 60-80% size reduction (especially for text/JSON-like data)
- **Universal support**: Standard compression format
- **Good CPU/bandwidth trade-off**: Compression cost worth the network savings

---

### Complete Data Flow

**Sending a Message:**

```
Application Data (Go struct)
    ↓
┌─────────────────────────┐
│ Layer 1: Protobuf       │
│ proto.Marshal(msg)      │
│ → [protobuf bytes]      │  200 bytes
└──────────┬──────────────┘
           │
           ↓
┌─────────────────────────┐
│ Layer 2: Metadata +     │
│ GOB Encoding            │
│                         │
│ GeneralWsReq {          │
│   ReqIdentifier: 1001   │ ← Metadata
│   SendID: "user123"     │ ← Metadata
│   OperationID: "op_abc" │ ← Metadata
│   Data: [proto bytes]   │ ← Layer 1 data
│ }                       │
│                         │
│ encoder.Encode(req)     │
│ → [GOB bytes]           │  220 bytes
└──────────┬──────────────┘
           │
           ↓
┌─────────────────────────┐
│ Layer 3: Gzip           │
│ compressor.Compress()   │
│ → [compressed bytes]    │  60 bytes (73% reduction!)
└──────────┬──────────────┘
           │
           ↓
    WebSocket Network
```

**Receiving a Message (Reverse Process):**

```
    WebSocket Network
           │
           ↓
┌─────────────────────────┐
│ Layer 3: Gzip           │
│ compressor.Decompress() │
│ ← [compressed bytes]    │  60 bytes
│ → [GOB bytes]           │  220 bytes
└──────────┬──────────────┘
           │
           ↓
┌─────────────────────────┐
│ Layer 2: GOB Decoding   │
│ encoder.Decode(resp)    │
│ ← [GOB bytes]           │
│                         │
│ GeneralWsResp {         │
│   ReqIdentifier: 1001   │
│   ErrCode: 0            │
│   Data: [proto bytes]   │ ← Extract protobuf
│ }                       │
└──────────┬──────────────┘
           │
           ↓
┌─────────────────────────┐
│ Layer 1: Protobuf       │
│ proto.Unmarshal(data)   │
│ ← [protobuf bytes]      │  200 bytes
│ → Application Data      │
└─────────────────────────┘
           │
           ↓
    Application Logic
```

**Key Point:** The protobuf data is the **inner core**, wrapped with metadata in a GOB-encoded struct, then compressed with gzip for transmission. The entire wrapped datagram (metadata + protobuf data) is encoded and compressed together.

---

### Message Structures

**Location:** `internal/interaction/ws_resp_asyn.go:28-44`

#### Request Message Structure

```go
type GeneralWsReq struct {
    ReqIdentifier int    `json:"reqIdentifier"`  // Message type ID
    Token         string `json:"token"`          // Auth token (optional)
    SendID        string `json:"sendID"`         // Sender user ID
    OperationID   string `json:"operationID"`    // Request tracing ID
    MsgIncr       string `json:"msgIncr"`        // Unique message ID
    Data          []byte `json:"data"`           // Protobuf-encoded payload
}
```

**Fields:**
- `ReqIdentifier` - Identifies the operation type (SendMsg, GetSeq, PullMsg, etc.)
- `Token` - Authentication token (may be empty for some operations)
- `SendID` - User ID of the sender
- `OperationID` - Unique ID for distributed tracing and logging
- `MsgIncr` - Unique message identifier for request-response matching
- `Data` - Protobuf-encoded business data

#### Response Message Structure

```go
type GeneralWsResp struct {
    ReqIdentifier int    `json:"reqIdentifier"`  // Message type ID (matches request)
    ErrCode       int    `json:"errCode"`        // Error code (0 = success)
    ErrMsg        string `json:"errMsg"`         // Error message
    MsgIncr       string `json:"msgIncr"`        // Matches request MsgIncr
    OperationID   string `json:"operationID"`    // Matches request OperationID
    Data          []byte `json:"data"`           // Protobuf-encoded response
}
```

**Fields:**
- `ReqIdentifier` - Same as request, for routing
- `ErrCode` - 0 for success, non-zero for errors
- `ErrMsg` - Human-readable error description
- `MsgIncr` - Used to match response to request
- `OperationID` - For tracing across request/response
- `Data` - Protobuf-encoded response payload

---

### Encoding: GOB Format

**Location:** `internal/interaction/encoder.go`

**GOB (Go Binary)** is Go's native binary serialization format, similar to Java's serialization or Python's pickle.

#### Encoder Interface

```go
type Encoder interface {
    Encode(data interface{}) ([]byte, error)       // Serialize to bytes
    Decode(encodeData []byte, decodeData interface{}) error  // Deserialize from bytes
}
```

#### GobEncoder Implementation

```go
type GobEncoder struct {
}

func NewGobEncoder() *GobEncoder {
    return &GobEncoder{}
}
```

#### Encode Method

**Location:** `encoder.go:34-42`

```go
func (g *GobEncoder) Encode(data interface{}) ([]byte, error) {
    buff := bytes.Buffer{}           // Create in-memory buffer
    enc := gob.NewEncoder(&buff)     // Create GOB encoder
    err := enc.Encode(data)          // Encode data to buffer
    if err != nil {
        return nil, err
    }
    return buff.Bytes(), nil         // Return serialized bytes
}
```

**Process:**
1. Create a bytes buffer to hold encoded data
2. Create GOB encoder that writes to buffer
3. Encode the data structure (GeneralWsReq)
4. Extract bytes from buffer
5. Return serialized binary data

**Example:**
```go
req := GeneralWsReq{
    ReqIdentifier: constant.SendMsg,
    SendID:        "user123",
    OperationID:   "op_12345",
    Data:          protoData,  // Already protobuf-encoded
}

encoder := NewGobEncoder()
encodedBytes, err := encoder.Encode(req)
// encodedBytes is now GOB-encoded binary data
```

#### Decode Method

**Location:** `encoder.go:43-51`

```go
func (g *GobEncoder) Decode(encodeData []byte, decodeData interface{}) error {
    buff := bytes.NewBuffer(encodeData)  // Create buffer from bytes
    dec := gob.NewDecoder(buff)          // Create GOB decoder
    err := dec.Decode(decodeData)        // Decode into target struct
    if err != nil {
        return errs.Wrap(err)
    }
    return nil
}
```

**Process:**
1. Create buffer from encoded bytes
2. Create GOB decoder that reads from buffer
3. Decode into provided data structure (GeneralWsResp)
4. Return error if decoding fails

**Example:**
```go
var resp GeneralWsResp
encoder := NewGobEncoder()
err := encoder.Decode(receivedBytes, &resp)
// resp is now populated with decoded data
```

**Why GOB?**
- **Type-safe**: Preserves Go types during serialization
- **Efficient**: More compact than JSON
- **Fast**: Native Go format with good performance
- **Flexible**: Handles complex nested structures

---

### Compression: Gzip

**Location:** `internal/interaction/compressor.go`

**Gzip compression** reduces message size for efficient network transmission, especially important for large messages or limited bandwidth.

#### Compressor Interface

```go
type Compressor interface {
    Compress(rawData []byte) ([]byte, error)
    CompressWithPool(rawData []byte) ([]byte, error)
    DeCompress(compressedData []byte) ([]byte, error)
    DecompressWithPool(compressedData []byte) ([]byte, error)
}
```

**Two versions:**
- **Standard**: Creates new compressor each time
- **WithPool**: Uses object pooling for better performance (preferred)

#### Object Pooling

**Location:** `compressor.go:26-29`

```go
var (
    gzipWriterPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}
    gzipReaderPool = sync.Pool{New: func() any { return new(gzip.Reader) }}
)
```

**Purpose of `sync.Pool`:**
- Reuses gzip compressor/decompressor objects
- Reduces allocations and GC pressure
- Improves performance for high-throughput scenarios
- Thread-safe concurrent access

**How `sync.Pool` Works:**
```
First call:
    Pool is empty → New() creates fresh object
    Use object
    Put() returns object to pool

Subsequent calls:
    Get() retrieves existing object from pool
    Use object (after Reset())
    Put() returns object to pool

Benefits:
    - No repeated allocations
    - Lower memory usage
    - Reduced garbage collection overhead
```

#### GzipCompressor Implementation

```go
type GzipCompressor struct {
    compressProtocol string  // "gzip"
}

func NewGzipCompressor() *GzipCompressor {
    return &GzipCompressor{compressProtocol: "gzip"}
}
```

#### Compress With Pool (Preferred)

**Location:** `compressor.go:58-72`

```go
func (g *GzipCompressor) CompressWithPool(rawData []byte) ([]byte, error) {
    // 1. Get writer from pool (reuse existing object)
    gz := gzipWriterPool.Get().(*gzip.Writer)
    defer gzipWriterPool.Put(gz)  // Return to pool when done

    // 2. Create output buffer
    gzipBuffer := bytes.Buffer{}

    // 3. Reset writer to target new buffer
    gz.Reset(&gzipBuffer)

    // 4. Write raw data (compresses automatically)
    if _, err := gz.Write(rawData); err != nil {
        return nil, errs.WrapMsg(err, "")
    }

    // 5. Close writer (flushes remaining data)
    if err := gz.Close(); err != nil {
        return nil, errs.WrapMsg(err, "")
    }

    // 6. Return compressed bytes
    return gzipBuffer.Bytes(), nil
}
```

**Key Steps:**
1. **Get from pool** - Retrieves reusable writer (or creates new)
2. **Defer Put** - Ensures writer returns to pool (even on error)
3. **Create buffer** - Fresh buffer for compressed output
4. **Reset writer** - Reconfigures writer for new buffer
5. **Write data** - Compresses data into buffer
6. **Close** - Finalizes compression (important!)
7. **Return bytes** - Compressed data ready to send

**Example:**
```go
compressor := NewGzipCompressor()
compressed, err := compressor.CompressWithPool(encodedData)
// compressed is now gzip-compressed binary data

// Typical compression ratios:
// Text: 60-80% reduction
// JSON: 70-85% reduction
// Already compressed (images): minimal reduction
```

#### Decompress With Pool (Preferred)

**Location:** `compressor.go:88-106`

```go
func (g *GzipCompressor) DecompressWithPool(compressedData []byte) ([]byte, error) {
    // 1. Get reader from pool
    reader := gzipReaderPool.Get().(*gzip.Reader)
    if reader == nil {
        return nil, errors.New("NewReader failed")
    }
    defer gzipReaderPool.Put(reader)  // Return to pool when done

    // 2. Reset reader with compressed data
    err := reader.Reset(bytes.NewReader(compressedData))
    if err != nil {
        return nil, errs.WrapMsg(err, "NewReader failed")
    }

    // 3. Read all decompressed data
    compressedData, err = io.ReadAll(reader)
    if err != nil {
        return nil, errs.WrapMsg(err, "ReadAll failed")
    }

    // 4. Close reader
    _ = reader.Close()

    // 5. Return decompressed bytes
    return compressedData, nil
}
```

**Key Steps:**
1. **Get from pool** - Retrieves reusable reader
2. **Defer Put** - Returns reader to pool
3. **Reset reader** - Configure for new compressed data
4. **Read all** - Decompresses entire stream
5. **Close** - Cleanup
6. **Return bytes** - Original uncompressed data

#### Standard Methods (Without Pool)

**Compress** - `compressor.go:46-56`
```go
func (g *GzipCompressor) Compress(rawData []byte) ([]byte, error) {
    gzipBuffer := bytes.Buffer{}
    gz := gzip.NewWriter(&gzipBuffer)  // Creates NEW writer (not pooled)
    if _, err := gz.Write(rawData); err != nil {
        return nil, errs.WrapMsg(err, "")
    }
    if err := gz.Close(); err != nil {
        return nil, errs.WrapMsg(err, "")
    }
    return gzipBuffer.Bytes(), nil
}
```

**DeCompress** - `compressor.go:74-86`
```go
func (g *GzipCompressor) DeCompress(compressedData []byte) ([]byte, error) {
    buff := bytes.NewBuffer(compressedData)
    reader, err := gzip.NewReader(buff)  // Creates NEW reader (not pooled)
    if err != nil {
        return nil, errs.WrapMsg(err, "NewReader failed")
    }
    compressedData, err = io.ReadAll(reader)
    if err != nil {
        return nil, errs.WrapMsg(err, "ReadAll failed")
    }
    _ = reader.Close()
    return compressedData, nil
}
```

**When to Use:**
- **WithPool**: High-frequency operations (default choice)
- **Without Pool**: One-off operations or testing

---

### Outbound Message Pipeline

**Location:** `long_conn_mgr.go:436-454`

#### Complete Send Flow

```go
func (c *LongConnMgr) writeBinaryMsgNoLock(req GeneralWsReq) error {
    // STAGE 1: GOB ENCODING
    encodeBuf, err := c.encoder.Encode(req)
    if err != nil {
        return err
    }

    // STAGE 2: CONNECTION CHECK
    if c.GetConnectionStatus() != Connected {
        return sdkerrs.ErrNetwork.WrapMsg("connection closed,re conning...")
    }

    // STAGE 3: SET WRITE DEADLINE
    _ = c.conn.SetWriteDeadline(writeWait)

    // STAGE 4: GZIP COMPRESSION (if enabled)
    if c.IsCompression {
        resultBuf, compressErr := c.compressor.CompressWithPool(encodeBuf)
        if compressErr != nil {
            return compressErr
        }
        return c.conn.WriteMessage(MessageBinary, resultBuf)
    } else {
        // No compression - send encoded data directly
        return c.conn.WriteMessage(MessageBinary, encodeBuf)
    }
}
```

**Pipeline Visualization:**

```
┌─────────────────────────────────────────────────────────────────┐
│                  OUTBOUND MESSAGE PIPELINE                      │
└─────────────────────────────────────────────────────────────────┘

Step 1: Application Data
    ↓
    Business Logic (e.g., SendMessage)
    ↓
┌───────────────────────┐
│ Protobuf Encoding     │  ← Convert Go struct to protobuf bytes
│ proto.Marshal(msg)    │
└───────────┬───────────┘
            │ [protobuf bytes]
            ↓
┌───────────────────────┐
│ Create GeneralWsReq   │  ← Wrap in request structure
│ {                     │
│   ReqIdentifier: 1001 │
│   Data: [proto bytes] │
│   ...                 │
│ }                     │
└───────────┬───────────┘
            │ [GeneralWsReq struct]
            ↓
┌───────────────────────┐
│ GOB Encoding          │  ← Serialize request struct
│ encoder.Encode(req)   │
└───────────┬───────────┘
            │ [GOB-encoded bytes] (~original size)
            ↓
┌───────────────────────┐
│ Gzip Compression      │  ← Compress encoded bytes
│ compressor.Compress() │     (if c.IsCompression = true)
└───────────┬───────────┘
            │ [compressed bytes] (60-80% smaller)
            ↓
┌───────────────────────┐
│ WebSocket Binary Frame│  ← Send over network
│ conn.WriteMessage()   │
└───────────┬───────────┘
            │
            ↓
         Network
```

**Size Reduction Example:**
```
Original protobuf data:    5,000 bytes
After GOB encoding:        5,100 bytes  (slight overhead for struct)
After gzip compression:    1,200 bytes  (76% reduction)
                           ↓
Sent over network:         1,200 bytes  (saves 3,800 bytes!)
```

---

### Inbound Message Pipeline

**Location:** `long_conn_mgr.go:467-482`

#### Complete Receive Flow

```go
func (c *LongConnMgr) handleMessage(message []byte) error {
    // STAGE 1: GZIP DECOMPRESSION (if compression enabled)
    if c.IsCompression {
        var decompressErr error
        message, decompressErr = c.compressor.DecompressWithPool(message)
        if decompressErr != nil {
            log.ZError(c.ctx, "DeCompress failed", decompressErr, message)
            return sdkerrs.ErrMsgDeCompression
        }
    }

    // STAGE 2: GOB DECODING
    var wsResp GeneralWsResp
    err := c.encoder.Decode(message, &wsResp)
    if err != nil {
        log.ZError(c.ctx, "decodeBinaryWs err", err, "message", message)
        return sdkerrs.ErrMsgDecodeBinaryWs
    }

    // STAGE 3: ROUTING (by ReqIdentifier)
    ctx := context.WithValue(c.ctx, "operationID", wsResp.OperationID)
    log.ZInfo(ctx, "recv msg", "errCode", wsResp.ErrCode, "errMsg", wsResp.ErrMsg,
        "reqIdentifier", wsResp.ReqIdentifier)

    switch wsResp.ReqIdentifier {
    case constant.PushMsg:
        // New message push from server
        c.doPushMsg(ctx, wsResp)
    case constant.SendMsg:
        // Response to sent message
        c.Syncer.NotifyResp(ctx, wsResp)
    // ... other cases
    }

    return nil
}
```

**Pipeline Visualization:**

```
┌─────────────────────────────────────────────────────────────────┐
│                  INBOUND MESSAGE PIPELINE                       │
└─────────────────────────────────────────────────────────────────┘

         Network
            │
            ↓
┌───────────────────────┐
│ WebSocket Binary Frame│  ← Receive from network
│ conn.ReadMessage()    │
└───────────┬───────────┘
            │ [binary bytes] (compressed)
            ↓
┌───────────────────────┐
│ Gzip Decompression    │  ← Decompress
│ compressor.Decompress()│     (if c.IsCompression = true)
└───────────┬───────────┘
            │ [GOB-encoded bytes]
            ↓
┌───────────────────────┐
│ GOB Decoding          │  ← Deserialize response struct
│ encoder.Decode(resp)  │
└───────────┬───────────┘
            │ [GeneralWsResp struct]
            │ {
            │   ReqIdentifier: 1001
            │   ErrCode: 0
            │   Data: [proto bytes]
            │   ...
            │ }
            ↓
┌───────────────────────┐
│ Route by Identifier   │  ← Determine message type
│ switch ReqIdentifier  │
└───────────┬───────────┘
            │
    ┌───────┼───────┬───────────┐
    ↓       ↓       ↓           ↓
 PushMsg SendMsg GetSeq    KickOnline
    │       │       │           │
    ↓       ↓       ↓           ↓
┌────────────────────────────────┐
│ Protobuf Decoding              │  ← Decode business data
│ proto.Unmarshal(wsResp.Data)   │
└───────────┬────────────────────┘
            │ [Application Data]
            ↓
    Application Logic
```

---

### Compression Configuration

#### Enabling Compression

**Location:** WebSocket URL during connection

```go
// In reConn() function
url := fmt.Sprintf("%s?sendID=%s&token=%s&platformID=%d&operationID=%s&isBackground=%t",
    wsAddr, userID, token, platformID, operationID, isBackground)

if c.IsCompression {
    url += fmt.Sprintf("&compression=%s", "gzip")  // Signal server to use compression
}
```

**How it works:**
1. Client sends `compression=gzip` parameter during WebSocket handshake
2. Server acknowledges and enables compression
3. Both client and server use gzip for all messages
4. `c.IsCompression` flag controls local compression logic

#### Setting IsCompression

**Location:** `long_conn_mgr.go` initialization

```go
type LongConnMgr struct {
    // ...
    IsCompression bool  // Flag to enable/disable compression
    compressor    Compressor  // Gzip compressor instance
    encoder       Encoder     // GOB encoder instance
    // ...
}

// During initialization
func NewLongConnMgr(...) *LongConnMgr {
    return &LongConnMgr{
        IsCompression: true,  // Enable compression
        compressor:    NewGzipCompressor(),
        encoder:       NewGobEncoder(),
        // ...
    }
}
```

---

### Performance Characteristics

#### Encoding Performance

**GOB Encoding:**

| Metric | Performance |
|--------|-------------|
| Speed | ~100-200 MB/s (depending on data complexity) |
| Overhead | ~5-15% size increase for struct metadata |
| CPU Usage | Low (native Go format) |

**When GOB is efficient:**
- Complex nested structures
- Many fields
- Type preservation needed

**When GOB is less efficient:**
- Simple flat data (JSON might be smaller)
- Cross-language communication (use protobuf/JSON instead)

#### Compression Performance

**Gzip Compression:**

| Metric | Performance |
|--------|-------------|
| Speed | ~50-100 MB/s compression, ~200-300 MB/s decompression |
| Ratio | 60-80% size reduction (text/JSON), 40-60% (binary) |
| CPU Usage | Medium (worth it for network savings) |

**Compression effectiveness:**

| Data Type | Typical Reduction |
|-----------|-------------------|
| Text messages | 70-85% |
| JSON data | 75-85% |
| Protobuf (already compact) | 40-60% |
| Images/video (already compressed) | 0-10% |

**Break-even point:**
- Messages < 1 KB: Compression overhead may not be worth it
- Messages > 5 KB: Compression almost always beneficial
- SDK uses compression for all messages when enabled

#### Object Pooling Benefits

**sync.Pool Performance:**

| Scenario | Without Pool | With Pool | Improvement |
|----------|--------------|-----------|-------------|
| Allocations | 1 per message | ~0.1 per message | 10x fewer |
| GC pressure | High | Low | 5-10x reduction |
| Latency (p99) | +2-5ms | +0.5-1ms | 3-4x better |

**Benchmark example (1000 messages/sec):**
```
Without pooling:
  - Allocations: 1000 gzip.Writer objects/sec
  - GC cycles: Every 2-3 seconds
  - Memory: 50-100 MB

With pooling:
  - Allocations: ~20-50 gzip.Writer objects/sec (cache misses)
  - GC cycles: Every 10-15 seconds
  - Memory: 10-20 MB
```

---

### Error Handling

#### Encoding Errors

```go
encodeBuf, err := c.encoder.Encode(req)
if err != nil {
    // Rare - usually indicates:
    // - Struct contains unserializable types
    // - Memory allocation failed
    return err
}
```

**Common causes:**
- Struct contains channels (not serializable)
- Struct contains functions (not serializable)
- Out of memory

#### Compression Errors

```go
resultBuf, compressErr := c.compressor.CompressWithPool(encodeBuf)
if compressErr != nil {
    // Usually indicates:
    // - Corrupted input data
    // - Memory allocation failed
    return compressErr
}
```

**Common causes:**
- Very large messages (>1GB)
- Out of memory

#### Decompression Errors

```go
message, decompressErr = c.compressor.DecompressWithPool(message)
if decompressErr != nil {
    log.ZError(c.ctx, "DeCompress failed", decompressErr, message)
    return sdkerrs.ErrMsgDeCompression
}
```

**Common causes:**
- Corrupted network data
- Not actually compressed (compression mismatch)
- Truncated message

#### Decoding Errors

```go
var wsResp GeneralWsResp
err := c.encoder.Decode(message, &wsResp)
if err != nil {
    log.ZError(c.ctx, "decodeBinaryWs err", err, "message", message)
    return sdkerrs.ErrMsgDecodeBinaryWs
}
```

**Common causes:**
- Corrupted data after decompression
- Version mismatch (struct changed)
- Not actually GOB-encoded data

---

### Complete Pipeline Example

#### Sending a Message

```go
// 1. Application creates protobuf message
textMsg := &sdkws.MsgData{
    Content: []byte("Hello, world!"),
    // ... other fields
}
protoData, _ := proto.Marshal(textMsg)  // ~200 bytes

// 2. Wrap in GeneralWsReq
req := GeneralWsReq{
    ReqIdentifier: constant.SendMsg,
    SendID:        "user123",
    OperationID:   "op_abc",
    Data:          protoData,  // 200 bytes
}

// 3. GOB encode
encoder := NewGobEncoder()
gobData, _ := encoder.Encode(req)  // ~220 bytes (struct overhead)

// 4. Gzip compress
compressor := NewGzipCompressor()
compressed, _ := compressor.CompressWithPool(gobData)  // ~60 bytes (73% reduction!)

// 5. Send over WebSocket
conn.WriteMessage(MessageBinary, compressed)
// Total sent: 60 bytes instead of 220 bytes
```

#### Receiving a Message

```go
// 1. Receive from WebSocket
messageType, data, _ := conn.ReadMessage()  // data = 60 bytes (compressed)

// 2. Gzip decompress
compressor := NewGzipCompressor()
decompressed, _ := compressor.DecompressWithPool(data)  // 220 bytes (original GOB)

// 3. GOB decode
encoder := NewGobEncoder()
var resp GeneralWsResp
encoder.Decode(decompressed, &resp)
// resp.Data = protobuf bytes (200 bytes)

// 4. Protobuf decode
var msgData sdkws.MsgData
proto.Unmarshal(resp.Data, &msgData)
// msgData.Content = "Hello, world!"
```

---

### Key Takeaways

#### 1. **Two-Stage Pipeline**
- **Stage 1**: GOB encoding (serialization)
- **Stage 2**: Gzip compression (size reduction)

#### 2. **Object Pooling**
- Reuses compressor/decompressor objects
- Reduces allocations and GC pressure
- Significant performance improvement

#### 3. **Compression is Optional**
- Controlled by `c.IsCompression` flag
- Negotiated during WebSocket handshake
- Can be disabled for debugging

#### 4. **Error Handling**
- Encoding errors are rare (struct issues)
- Compression errors indicate data corruption
- Errors trigger reconnection and retry

#### 5. **Performance Trade-offs**
- Encoding: Fast, minimal CPU overhead
- Compression: Moderate CPU, major bandwidth savings
- Net benefit: Almost always positive (especially on mobile)

#### 6. **Thread Safety**
- `sync.Pool` is thread-safe
- Each message processed independently
- No shared state between compressions
