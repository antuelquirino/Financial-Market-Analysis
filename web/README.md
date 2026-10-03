# Financial Market web

The dashboard of Financial Market, in Next.js 16 (App Router), React 19,
Tailwind CSS 4 and Recharts. See the [project README](../README.md) for what
it shows and how the pieces fit.

```bash
cp .env.example .env.local   # where the API is (http://localhost:8765)
npm install
npm run dev                  # http://localhost:3000
npm test                     # unit tests (Vitest)
npm run lint && npm run typecheck && npm run build
```

- `src/app/`: the three screens (`/`, `/risk`, `/sectors`), `/styleguide`, and
  the design tokens in `globals.css`.
- `src/components/market/`: this project's components: header with the global
  selection, lead finding, KPI strip, charts, sector table, states.
- `src/components/`: Tremor Raw components (MIT, see `LICENSE.md`).
- `src/lib/`: the API client and types, `format.ts` (the only place a number
  becomes text), and one module per screen (`overview.ts`, `risk.ts`,
  `sectors.ts`) with the sentences and data each one shows, all unit-tested.

The design system is shared with InsightFlow; this project's accent is cobalt.
