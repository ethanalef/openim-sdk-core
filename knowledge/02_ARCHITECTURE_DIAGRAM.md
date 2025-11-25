================================================================================
                    OPENIM SDK CORE - ARCHITECTURE DIAGRAM
================================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│                         APPLICATION LAYER                                   │
│                  (iOS/Android/Web/PC - Client Code)                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
        ┌──────────────────┐ ┌────────┐ ┌─────────────┐
        │   Go Native      │ │  WASM  │ │ C Bindings  │
        │   Callbacks      │ │  JS    │ │   (Obj-C)   │
        │   Interface      │ │Bridge  │ │   (JNI)     │
        └────────┬─────────┘ └───┬────┘ └─────┬───────┘
                 │               │            │
                 └───────┬───────┴────────────┘
                         │
        ┌────────────────▼────────────────────────────────┐
        │   open_im_sdk/ - PUBLIC SDK INTERFACE          │
        │  ┌─────────────────────────────────────────┐  │
        │  │  init_login.go                          │  │
        │  │  ├─ InitSDK()                           │  │
        │  │  ├─ Login() / Logout()                  │  │
        │  │  └─ NetworkStatusChanged()              │  │
        │  ├─ caller.go (Reflection RPC)             │  │
        │  │  ├─ call()      - Async callbacks       │  │
        │  │  ├─ syncCall()  - Sync with return      │  │
        │  │  └─ messageCall() - Message ops         │  │
        │  ├─ apicb.go (Error handling)              │  │
        │  │  ├─ OnUserTokenExpired()                │  │
        │  │  ├─ OnUserTokenInvalid()                │  │
        │  │  └─ OnKickedOffline()                   │  │
        │  ├─ conversation_msg.go (Message facade)   │  │
        │  ├─ group.go (Group facade)                │  │
        │  ├─ user.go (User facade)                  │  │
        │  ├─ relation.go (Friend facade)            │  │
        │  └─ userRelated.go (LoginMgr singleton)    │  │
        │      └─ LoginMgr struct (central hub)      │  │
        └────────────────┬─────────────────────────────┘
                         │
        ┌────────────────▼────────────────────────────────┐
        │  internal/ - BUSINESS LOGIC LAYER               │
        │                                                 │
        │  ┌──────────────────────────────────────────┐  │
        │  │  interaction/                            │  │
        │  │  ├─ long_conn_mgr.go                     │  │
        │  │  │  ├─ readPump()   - Read goroutine    │  │
        │  │  │  ├─ writePump()  - Write goroutine   │  │
        │  │  │  ├─ heartbeat()  - Ping goroutine    │  │
        │  │  │  ├─ reconnect strategy (exp. backoff)│  │
        │  │  │  └─ compression/encoding (gzip,GOB)  │  │
        │  │  ├─ long_connection.go (LongConn I/F)  │  │
        │  │  ├─ msg_sync.go (Message sync hub)      │  │
        │  │  │  ├─ SEQ tracking per conversation    │  │
        │  │  │  ├─ Incremental sync                 │  │
        │  │  │  ├─ Message gap detection            │  │
        │  │  │  └─ Foreground/background handling   │  │
        │  │  └─ ws_resp_asyn.go (Response tracking) │  │
        │  │                                         │  │
        │  ├─ conversation_msg/                      │  │
        │  │  ├─ conversation.go (Conversation mgmt) │  │
        │  │  ├─ conversation_msg.go (Message ops)   │  │
        │  │  ├─ create_message.go (All msg types)   │  │
        │  │  ├─ incremental_sync.go (SEQ sync)      │  │
        │  │  └─ message_check.go (Validation)       │  │
        │  │                                         │  │
        │  ├─ group/ (Group operations)              │  │
        │  ├─ user/ (User profile & status)          │  │
        │  ├─ relation/ (Friends & blacklist)        │  │
        │  └─ third/ (File uploads, APIs)            │  │
        │                                            │  │
        └────────┬─────────────────────────────────────┘
                 │
        ┌────────▼──────────────────────────────────────┐
        │  pkg/ - SHARED UTILITIES                      │
        │                                               │
        │  ┌─────────────────────────────────────────┐ │
        │  │  db/ & db_interface/ (Data Layer)       │ │
        │  │  ├─ DataBase interface (9 sub-I/F)     │ │
        │  │  ├─ GroupModel       - Group CRUD      │ │
        │  │  ├─ MessageModel     - Message CRUD    │ │
        │  │  ├─ ConversationModel- Conv CRUD       │ │
        │  │  ├─ UserModel        - User CRUD       │ │
        │  │  ├─ FriendModel      - Friend CRUD     │ │
        │  │  └─ db_init.go       - Schema setup    │ │
        │  │      └─ SQLite via GORM ORM            │ │
        │  │         Per-user DB: dataDir/userId/   │ │
        │  │                      im_platformId.db   │ │
        │  │                                        │ │
        │  ├─ cache/                                │ │
        │  │  ├─ Cache[K,V] generic                │ │
        │  │  └─ sync.Map based implementation     │ │
        │  │                                        │ │
        │  ├─ api/ (REST API Client)               │ │
        │  │  ├─ Proto message definitions         │ │
        │  │  ├─ User API calls                    │ │
        │  │  ├─ Message API calls                 │ │
        │  │  ├─ Group API calls                   │ │
        │  │  └─ Conversation API calls            │ │
        │  │                                        │ │
        │  ├─ syncer/ (Message sync engine)        │ │
        │  ├─ sdkerrs/ (Error types)               │ │
        │  ├─ constant/ (Enums & constants)        │ │
        │  ├─ utils/ (Helpers & utilities)         │ │
        │  └─ network/ (Network utilities)         │ │
        │                                          │ │
        └──────────────┬───────────────────────────┘ │
                       │                              │
        ┌──────────────▼──────────────────────────┐  │
        │ open_im_sdk_callback/ (Event System)   │  │
        │ ├─ OnConnListener (Connection)         │  │
        │ ├─ OnAdvancedMsgListener (Messages)    │  │
        │ ├─ OnBatchMsgListener (Batch msgs)     │  │
        │ ├─ OnConversationListener (Conv)       │  │
        │ ├─ OnGroupListener (Groups)            │  │
        │ ├─ OnFriendshipListener (Friends)      │  │
        │ ├─ OnUserListener (Users)              │  │
        │ └─ OnCustomBusinessListener (Custom)   │  │
        │                                        │  │
        └────────────────────────────────────────┘  │
        │                                            │
        └────────────────────────────────────────────┘
                        │
                        ▼
        ┌────────────────────────────────────┐
        │     WASM SPECIFIC (js && wasm)    │
        │                                    │
        │  wasm/                             │
        │  ├─ indexdb/ (Browser storage)    │
        │  │  ├─ Same schema as SQLite      │
        │  │  ├─ Async JS calls             │
        │  │  └─ IndexedDB implementation   │
        │  ├─ wasm_wrapper/                 │
        │  │  ├─ wasm_init_login.go         │
        │  │  ├─ wasm_conversation_msg.go   │
        │  │  ├─ wasm_group.go              │
        │  │  ├─ wasm_user.go               │
        │  │  └─ wasm_friend.go             │
        │  ├─ event_listener/               │
        │  │  ├─ listener.go (Callbacks)    │
        │  │  ├─ caller.go (JS bridge)      │
        │  │  └─ callback_writer.go (JSON)  │
        │  └─ cmd/main.go                   │
        │     └─ Register ~100 JS globals   │
        │                                    │
        └────────────────────────────────────┘
                        │
                        ▼
        ┌────────────────────────────────────┐
        │    EXTERNAL SYSTEMS                │
        │                                    │
        │  ┌──────────────────────────────┐  │
        │  │  OpenIM Server               │  │
        │  │  (gRPC API, WebSocket Push)  │  │
        │  └──────────────────────────────┘  │
        │                                    │
        │  ┌──────────────────────────────┐  │
        │  │  Third-party Services        │  │
        │  │  (File storage, FCM, etc.)   │  │
        │  └──────────────────────────────┘  │
        │                                    │
        └────────────────────────────────────┘

================================================================================
                           COMMUNICATION CHANNELS
================================================================================

                    ┌────────────────────────────┐
                    │       LoginMgr             │
                    │    (Central Hub)           │
                    └─────────┬──────────────────┘
                             ▼
        ┌───────────────────────────────────────────────────┐
        │                                                   │
    conversationCh │                          │ cmdWsCh      │ msgSyncerCh │ loginMgrCh
                  │                          │              │             │
                  ▼                          ▼              ▼             ▼
        ┌──────────────────┐      ┌─────────────────┐  ┌─────────┐  ┌───────────┐
        │ Conversation Mgr │      │ LongConnMgr     │  │MsgSyncer│  │ErrorHandle│
        │                  │      │ (WebSocket)     │  │         │  │(Token exp)│
        └──────────────────┘      └─────────────────┘  └─────────┘  └───────────┘

================================================================================
                    INITIALIZATION & LOGIN SEQUENCE
================================================================================

User Code
    │
    ├─ InitSDK(config, listener)
    │  └─ LoginMgr.InitSDK()
    │     ├─ Parse config (API, WS, platform, etc.)
    │     ├─ Initialize logger
    │     └─ Store listener reference
    │
    ├─ Login(userID, token)
    │  └─ LoginMgr.login()
    │     ├─ Create/initialize database
    │     ├─ Load SEQ state from DB
    │     ├─ Create managers (Conversation, Group, User, etc.)
    │     ├─ Create LongConnMgr (WebSocket)
    │     │  └─ Start read/write/heartbeat pumps
    │     ├─ Create MsgSyncer
    │     │  └─ Load conversation SEQ state
    │     ├─ Establish WebSocket connection
    │     ├─ Trigger message sync
    │     └─ Call OnConnectSuccess()
    │
    └─ Logout()
       └─ LoginMgr.logout()
          ├─ Close WebSocket
          ├─ Cancel context
          ├─ Close database
          └─ Cleanup listeners

================================================================================
                       WEBSOCKET MESSAGE FLOW
================================================================================

CLIENT                          WEBSOCKET                        SERVER
  │                                                               │
  ├─ Proto Message                                               │
  ├─ proto.Marshal()                                             │
  ├─ GOB Encode                                                  │
  ├─ gzip Compress                                               │
  └─ Send Binary Frame ────────────────────────────────────────→ │
                                                                  │
                                                            Process message
                                                            Send response
  │ ←──────────────────────────── Receive Binary Frame           │
  │                                                               │
  ├─ gzip Decompress                                             │
  ├─ GOB Decode                                                  │
  ├─ proto.Unmarshal()                                           │
  └─ Route to handler
     ├─ Update database
     ├─ Trigger callback
     └─ Update local state

Heartbeat (every 24s):
  Client sends Ping ────────────────────────────────────────────→ Server
  Client ←───────────────────────────────── Server responds Pong

================================================================================
                       MESSAGE SYNCHRONIZATION FLOW
================================================================================

1. On Login:
   MsgSyncer.loadSeq() 
   └─ For each conversation: load lastSyncedSeq from DB

2. Sync Trigger (reconnect, push, foreground):
   MsgSyncer.doSyncMsg()
   └─ For each conversation:
      ├─ lastSeq = syncedMaxSeqs[convID]
      ├─ Request: seq > lastSeq (batch 100)
      ├─ InsertMessages(ctx, messages)
      ├─ Update syncedMaxSeqs[convID] = newSeq
      ├─ Update conversation.LastMessage
      ├─ Trigger OnRecvNewMessage callback
      └─ Save SEQ to database

3. Gap Detection:
   If missing sequence numbers detected
   └─ Pull messages for gap range
      ├─ Fill database
      ├─ Trigger callbacks
      └─ Continue sync

================================================================================
                            CALLBACK FLOW
================================================================================

                         Server Push
                            │
                            ▼
                     LongConnMgr.readPump
                            │
                ┌───────────┼───────────┐
                │           │           │
                ▼           ▼           ▼
           Message      Notification   Sync
           Push         Event          Event
                │           │           │
                ▼           ▼           ▼
           Deserialize Proto Messages
                │           │           │
                ▼           ▼           ▼
           Update         Update       Trigger
           Database      Conversation  Message
                │           │          Sync
                ▼           ▼           │
           Trigger    Trigger     (continue flow)
           Callback   Callback
                │           │
         OnRecvNew  OnConversation
         Message    Changed

Final callbacks go through registered listener interfaces:
- OnAdvancedMsgListener
- OnConversationListener
- OnGroupListener
- OnFriendshipListener
- OnUserListener
- etc.

================================================================================
