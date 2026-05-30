# Design System Specification (Google Stitch Style)
**Project**: Dual-Broker SOTA Engine Dashboard  
**Status**: Verified Design Specification  

This file defines the layout rules, typography, components, and color variables for the front-end dashboard of the Dual-Broker SOTA Engine.

---

## 🎨 1. Design Tokens (CSS Custom Variables)

All design variables must be defined in the `:root` scope to guarantee visual consistency and enable quick layout theme overrides.

```css
:root {
  /* --- Color Palette: Deep Space Glassmorphism --- */
  --bg-main: #06070d;                   /* Very dark charcoal/blue void */
  --bg-card: rgba(13, 17, 34, 0.65);     /* Semi-transparent panel fill */
  --bg-card-hover: rgba(22, 28, 54, 0.8);/* Highlighted panel hover state */
  --border-card: rgba(255, 255, 255, 0.05);/* Glassmorphism subtle border */
  --border-card-hover: rgba(0, 242, 254, 0.25);/* Active glow border */
  
  /* --- Brand Accents & Gradients --- */
  --color-primary: #00f2fe;             /* Neon Cyan (active connections) */
  --color-secondary: #7f00ff;           /* Electric Violet (swarm/consensus) */
  --gradient-brand: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%);
  --gradient-glow: radial-gradient(circle, rgba(0, 242, 254, 0.15) 0%, rgba(0,0,0,0) 70%);

  /* --- Status Indicators --- */
  --color-success: #00e676;             /* Neon green (Buy signal, active) */
  --color-danger: #ff1744;              /* Hot red (Sell signal, errors) */
  --color-warning: #ffea00;             /* Amber (circuit breaker pending, warnings) */
  --color-muted: #64748b;               /* Grey slate text */
  --color-text-bright: #f8fafc;         /* Off-white text */

  /* --- Typography --- */
  --font-headers: 'Space Grotesk', system-ui, sans-serif;
  --font-body: 'Inter', system-ui, sans-serif;
  
  /* --- Spacing & Radii --- */
  --radius-lg: 16px;
  --radius-md: 10px;
  --radius-sm: 6px;
  --spacing-base: 8px;
  
  /* --- Effects --- */
  --shadow-glow-cyan: 0 0 20px rgba(0, 242, 254, 0.3);
  --shadow-glow-violet: 0 0 20px rgba(127, 0, 255, 0.25);
  --backdrop-blur: blur(20px);
}
```

---

## 📐 2. Structural & Layout Rules

*   **Responsive Grid**: Use a 3-column dashboard grid for desktop screens (`min-width: 1200px`), collapsing into 2 columns for tablets (`768px - 1199px`), and a single vertical stack on mobile screens (`max-width: 767px`).
*   **Viewport Constraints**: The dashboard should be designed to fit onto a single screen where possible (`100vh` container with independent scroll areas for the log feeds) to mimic an HFT command center.
*   **Glassmorphism Invariant**: Cards must employ:
    ```css
    background: var(--bg-card);
    backdrop-filter: var(--backdrop-blur);
    -webkit-backdrop-filter: var(--backdrop-blur);
    border: 1px solid var(--border-card);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    ```

---

## 🧩 3. Component Design Specs

### A. Top Connection Ribbon
*   **Grid layout**: 3 flex blocks (Bright Data, Alpaca, EVM Node).
*   **Pulse Indicators**: Green, yellow, or red pulse animation indicating connection status:
    ```css
    @keyframes pulse-glow {
      0% { box-shadow: 0 0 0 0 rgba(0, 230, 118, 0.7); }
      70% { box-shadow: 0 0 0 10px rgba(0, 230, 118, 0); }
      100% { box-shadow: 0 0 0 0 rgba(0, 230, 118, 0); }
    }
    ```

### B. Core Metrics Grid
*   **Metric Cards**: Large display numbers for key arbitrage values ($P_{swarm}$, $P_{posterior}$, Kelly sizing, Edge %, JSD).
*   **Visual Aid**: Micro-charts or progress bars showing the relative position inside Kelly sizing limits.

### C. The Swarm Consensus Matrix (50 Personas)
*   **Grid style**: A 5x10 or 10x5 compact grid of identical circle elements representing the 50 Swarm Analyst Personas.
*   **Interactive Node Spec**:
    *   **Colors**: Varying from bright cyan/green (YES/optimistic bias) to violet/red (NO/pessimistic bias).
    *   **Brightness/Opacity**: Relative to the persona's *Confidence Score* (more confident = more solid/brighter).
    *   **Hover**: Hovering a node displays a tooltip showing that persona's exact stats: `ID`, `Temperature`, `Bias`, and simulated consensus contribution.

### D. The Saga Workflow Log (Failover & Rollbacks)
*   **Visual Timeline**: Display of Legs.
    - Leg 1: Polymarket (Status indicator, USDC Amount, gas, slippage).
    - Leg 2: TradFi Hedge (Alpaca/IBKR execution status).
*   **Rollback State**: If Leg 2 fails, Leg 1 card turns red, and a visual link highlights the "COMPENSATING" transition reversing the execution with an animated red arrow.

---

## 🎨 4. Evasion of AI Slop (Guidelines)

To avoid generic, flat layouts:
1.  **Typography Contrast**: Use high-weight header font (Space Grotesk, 700) against highly readable body text (Inter, 400).
2.  **Harmonious Gradients**: Never use solid bright colors for backgrounds. Use subtle, dark, blurred gradient backdrops (`background: radial-gradient(circle at 10% 20%, ...)`) to create depth.
3.  **Micro-interactions**: Every button, card, and persona node must have custom transition parameters to feel responsive and premium.
