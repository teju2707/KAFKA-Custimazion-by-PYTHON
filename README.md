┌───────────────┬──────────┬──────────┬──────────────┐
│   Strategy    │  When?   │  Where?  │  Best For    │
├───────────────┼──────────┼──────────┼──────────────┤
│               │          │          │              │
│ Schema        │ BEFORE   │ Producer │ Prevention   │
│ Enforcement   │ sending  │ side     │ (stop bad    │
│               │          │          │  data early!)│
│               │          │          │              │
│ Dead Letter   │ AFTER    │ Consumer │ Recovery     │
│ Queue         │ failure  │ side     │ (save bad    │
│               │          │          │  messages)   │
│               │          │          │              │
│ Retry         │ DURING   │ Consumer │ Transient    │
│ Mechanism     │ failure  │ side     │ errors       │
│               │          │          │ (temporary)  │
│               │          │          │              │
│ Message       │ DURING   │ Consumer │ Noise        │
│ Filtering     │ process  │ side     │ removal      │
│               │          │          │ (extract     │
│               │          │          │  needed)     │
│               │          │          │              │
└───────────────┴──────────┴──────────┴──────────────┘
