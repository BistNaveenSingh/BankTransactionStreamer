# UI Design Specification: Bank Transaction Streamer

## 1. Visual Theme & Philosophy
- **Style**: Modern Light / White Enterprise Fintech (inspired by Stripe, Razorpay, PhonePe web consoles)
- **Primary Background**: `#f8fafc` (Slate 50)
- **Card / Surface Background**: `#ffffff` (Pure White) with subtle border `#e2e8f0` and soft elevation
- **Typography**: Inter / system sans-serif font stack
- **Icons & Symbols**: **Zero Emojis**. Replaced 100% with Google Material Symbols Outlined font and semantic vector symbols.

---

## 2. Color Tokens

| Token | Hex Value | Usage |
| :--- | :--- | :--- |
| `--bg-page` | `#f8fafc` | Page backdrop |
| `--bg-surface` | `#ffffff` | Cards, navbar, modal surfaces |
| `--bg-muted` | `#f1f5f9` | Step boxes, table headers, code blocks |
| `--border-color` | `#e2e8f0` | Card borders, dividers, table rows |
| `--border-focus` | `#3b82f6` | Input and button focus rings |
| `--text-primary` | `#0f172a` | Headers, table values, emphasis |
| `--text-secondary` | `#475569` | Subtitles, labels, secondary information |
| `--text-muted` | `#64748b` | Muted captions, placeholders |
| `--primary` | `#2563eb` | Action buttons, active badges, highlights |
| `--primary-light` | `#eff6ff` | Active step background, tag background |
| `--success` | `#059669` | Success badges, connected indicators |
| `--success-light` | `#ecfdf5` | Success alert background |
| `--danger` | `#dc2626` | Error alerts, disconnected indicators |
| `--danger-light` | `#fef2f2` | Error alert background |

---

## 3. Symbol / Icon Mapping (No Emojis)

| Component | Previous Emoji | New Material Symbol | Symbol Name |
| :--- | :---: | :---: | :--- |
| App Logo | ⚡ | 🏛️ (SVG) | `account_balance` |
| Kafka Broker | ⚡ | 🌐 | `hub` |
| MongoDB Database | ⚡ | 🗄️ | `database` |
| Message Stream | ⚡ | 🔄 | `sync_alt` |
| Payment Card Header | 💳 | 💳 | `payments` |
| Sender / Receiver | 👤 | 👤 | `person` / `person_outline` |
| Amount Input | 💰 | 💵 | `payments` |
| Send Payment Button | ⚡ | ➔ | `send` |
| Architecture Pipeline | 🔄 | 🌲 | `schema` |
| Event JSON Preview | 📄 | 🔣 | `data_object` |
| Stored Ledger Header | 📜 | 📊 | `table_chart` |
| Refresh Button | 🔄 | ↻ | `refresh` |
| Batch Simulation | ⚡ | ⚡ (symbol) | `dynamic_feed` |
| Success Alert | ✅ | ✓ | `check_circle` |
| Error Alert | ❌ | ✕ | `error` |

---

## 4. Layout Structure
1. **Header / Navbar**:
   - Clean white banner with elevation shadow
   - Logo, subtitle, and tag
   - 3 live status pills (Kafka Broker, MongoDB Server, Stream Message Count)
2. **Top Metric Cards (Phase 16 Analytics)**:
   - Total Transactions
   - Total Volume (₹)
   - Average Amount
   - Transaction Breakdown (UPI, Debit, Credit pills)
3. **Interactive 2-Column Section**:
   - Left: Digital Payment Simulator form with quick-fill amount chips (`₹100`, `₹500`, `₹2,000`, `₹10,000`)
   - Right: Real-time 5-step Pipeline visualizer (User &rarr; Producer &rarr; Kafka &rarr; Consumer &rarr; MongoDB) with active step highlight and live JSON payload viewer
4. **Stored Ledger Section (Phase 11 Retrieval)**:
   - Filter bar: Text search (ID / user), Type dropdown, Refresh, and Batch Simulator
   - Data table: Crisp white background, clean header, formatted Indian Rupee amounts (`en-IN`), status badges
