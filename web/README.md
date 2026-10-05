# Financial Market web

The dashboard of Financial Market, in Next.js 16 (App Router), React 19,
Tailwind CSS 4 and Recharts. See the [project README](../README.md) for what
it shows and how the pieces fit.

```bash
cp .env.example .env.local   # where the API is (http://localhost:8765)
npm install
npm run dev                  # http://localhost:3000 (English), /es (Spanish)
npm test                     # unit tests (Vitest)
npm run lint && npm run typecheck && npm run build
```

- `src/app/`: the routes, in English (`/`, `/risk`, `/sectors`) and Spanish
  (`/es`, `/es/risk`, `/es/sectors`), `/styleguide` (English only), and the
  design tokens in `globals.css`. `src/proxy.ts` sets `<html lang>`.
- `src/components/screens/`: each screen, rendered for either language.
- `src/components/market/`: this project's components: header with the global
  selection, lead finding, KPI strip, charts, sector table, states.
- `src/components/`: Tremor Raw components (MIT, see `LICENSE.md`).
- `src/lib/`: the API client and types, `i18n.ts` (every word in both
  languages), `format.ts` (the only place a number becomes text, per
  language), and one module per screen (`overview.ts`, `risk.ts`,
  `sectors.ts`) with the sentences and data each one shows, all unit-tested.

The design system is shared with InsightFlow; this project's accent is cobalt.
