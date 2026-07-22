# Design System

## Direction

Restrained product UI. Mood: a quiet traffic-control bench in a bright engineering lab.

## Color

Use OKLCH tokens from `src/styles.css`. Pure-white background, neutral ink, the palette seed's green for active/primary states, amber and red only for traffic semantics.

## Typography

One system sans family for UI and a system monospace for serialized machine data.

## Layout

Two-column desktop workbench collapsing to one column. The signal is the visual anchor; machine data remains compact and scannable.

## Components

Buttons use one shape and explicit hover, focus, active, disabled, and loading states. State labels accompany every color. Panels use borders without decorative shadows.

## Motion

Short 180ms state transitions only. Disable nonessential motion under `prefers-reduced-motion`.
