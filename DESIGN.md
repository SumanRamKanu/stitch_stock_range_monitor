---
name: Precision Markets
colors:
  surface: '#fbf8ff'
  surface-dim: '#d9d9e7'
  surface-bright: '#fbf8ff'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f3f2ff'
  surface-container: '#ededfb'
  surface-container-high: '#e7e7f5'
  surface-container-highest: '#e1e1ef'
  on-surface: '#191b25'
  on-surface-variant: '#434656'
  inverse-surface: '#2e303a'
  inverse-on-surface: '#f0effe'
  outline: '#737688'
  outline-variant: '#c3c5d9'
  surface-tint: '#004ced'
  primary: '#003ec7'
  on-primary: '#ffffff'
  primary-container: '#0052ff'
  on-primary-container: '#dfe3ff'
  inverse-primary: '#b7c4ff'
  secondary: '#505f76'
  on-secondary: '#ffffff'
  secondary-container: '#d0e1fb'
  on-secondary-container: '#54647a'
  tertiary: '#952200'
  on-tertiary: '#ffffff'
  tertiary-container: '#bf3003'
  on-tertiary-container: '#ffddd5'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#dde1ff'
  primary-fixed-dim: '#b7c4ff'
  on-primary-fixed: '#001452'
  on-primary-fixed-variant: '#0038b6'
  secondary-fixed: '#d3e4fe'
  secondary-fixed-dim: '#b7c8e1'
  on-secondary-fixed: '#0b1c30'
  on-secondary-fixed-variant: '#38485d'
  tertiary-fixed: '#ffdbd2'
  tertiary-fixed-dim: '#ffb4a1'
  on-tertiary-fixed: '#3c0800'
  on-tertiary-fixed-variant: '#891e00'
  background: '#fbf8ff'
  on-background: '#191b25'
  surface-variant: '#e1e1ef'
typography:
  display-lg:
    fontFamily: Hanken Grotesk
    fontSize: 32px
    fontWeight: '700'
    lineHeight: 40px
    letterSpacing: -0.02em
  headline-md:
    fontFamily: Hanken Grotesk
    fontSize: 20px
    fontWeight: '600'
    lineHeight: 28px
  body-md:
    fontFamily: Hanken Grotesk
    fontSize: 14px
    fontWeight: '400'
    lineHeight: 20px
  body-sm:
    fontFamily: Hanken Grotesk
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 18px
  label-mono:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: '500'
    lineHeight: 16px
    letterSpacing: 0.02em
  data-number:
    fontFamily: JetBrains Mono
    fontSize: 14px
    fontWeight: '600'
    lineHeight: 20px
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  unit: 4px
  container-padding: 24px
  gutter: 16px
  row-height-dense: 32px
  row-height-standard: 48px
---

## Brand & Style
The design system is engineered for high-performance financial environments where data density and clarity are paramount. The brand personality is **Professional, Analytical, and Dependable**, catering to active traders and financial analysts who require immediate cognitive processing of complex information.

The design style follows a **Corporate / Modern** aesthetic with a lean toward **Minimalism**. It avoids unnecessary ornamentation to prioritize "signal over noise." The UI employs a highly structured layout, a neutral foundational palette to prevent eye fatigue, and high-contrast status indicators to guide the user's attention to market movements.

## Colors
This design system utilizes a "Utility-First" color strategy. The palette is dominated by neutral whites and grays to create a clean canvas for data.

- **Primary Blue:** Used for interactive elements, primary actions, and focused states.
- **Success Green & Error Red:** Reserved strictly for performance indicators (price up/down) and system status. These must maintain a high WCAG contrast ratio against white backgrounds.
- **Surface & Backgrounds:** The `neutral_base` is used for the application backdrop, while `neutral_surface` (pure white) distinguishes cards and data tables. 
- **Bordering:** A consistent `neutral_border` is used to define table rows and container boundaries without adding visual weight.

## Typography
Typography is optimized for legibility and vertical alignment. **Hanken Grotesk** provides a modern, sharp sans-serif feel for the general UI and navigation. 

Crucially, this design system introduces **JetBrains Mono** for all numerical data and ticker symbols. Monospaced fonts are essential in financial dashboards to ensure that numbers align vertically in tables, allowing users to compare price points and percentages at a glance without visual shifting. 

- Use `data-number` for stock prices, quantities, and percentages.
- Use `label-mono` for ticker symbols (e.g., AAPL, BTC) and table headers.
- Use `display-lg` for portfolio totals and main section titles.

## Layout & Spacing
The layout follows a **Fixed Grid** approach for the side navigation (240px) and a **Fluid Grid** for the main content area. This ensures that data tables can expand to show more columns on wider monitors.

A strict 4px spacing scale is used to maintain mathematical harmony. 
- **Density:** Financial tables should offer a "Dense" toggle. Standard rows are 48px high, while dense rows are 32px to maximize data visibility.
- **Margins:** A standard 24px outer margin is applied to the main dashboard container.
- **Mobile Reflow:** On mobile, complex tables should pivot to "Card View" or utilize horizontal scrolling for the data area while keeping the "Symbol" column sticky.

## Elevation & Depth
To maintain a high-performance "Pro" feel, the design system avoids heavy shadows. Depth is achieved through **Tonal Layers** and **Low-Contrast Outlines**.

- **Level 0 (Background):** `neutral_base` (#F8FAFC).
- **Level 1 (Cards/Tables):** Pure white background with a 1px solid `neutral_border`. No shadow.
- **Level 2 (Modals/Popovers):** Pure white with a subtle, tight shadow (0px 4px 12px rgba(0,0,0,0.05)) to distinguish overlapping content.
- **Interactions:** Hover states on table rows should use a very subtle tint of the primary color at 4% opacity to indicate focus without obscuring data.

## Shapes
The design system uses a **Soft** shape language (4px - 8px radius). This creates a modern look that feels precise and technical rather than "playful."

- **Input Fields & Buttons:** 4px (rounded-sm) for a crisp, professional appearance.
- **Content Cards:** 8px (rounded-lg) to subtly soften the layout edges.
- **Status Chips:** 12px (rounded-xl) or fully pill-shaped to differentiate them from interactive buttons.

## Components
- **Data Tables:** The core of the system. Headers must be "Sticky." Sorting icons appear on hover. Use alternating row stripes (zebra striping) only in high-density views.
- **Sparkline Charts:** Simplified, borderless line charts used within table rows. Success Green if the period change is positive; Error Red if negative. No axes or labels on sparklines.
- **Filtering Chips:** Small, 24px height chips with a "Close" icon. Use `primary_color` at 10% opacity for the background and 100% for the text when active.
- **Primary Buttons:** Solid `primary_color_hex` with white text. High-contrast, sharp corners (4px).
- **Input Fields:** Minimalist design with a 1px `neutral_border`. Focus state uses a 1px `primary_color` border and a soft blue outer glow.
- **Price Badges:** Small containers for % change. Include a geometric "Up" (▲) or "Down" (▼) arrow icon to assist color-blind users.