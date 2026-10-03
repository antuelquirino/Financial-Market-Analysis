# InsightFlow web

The one-page dashboard of InsightFlow, in Next.js 16 (App Router), React 19,
Tailwind CSS 4 and Recharts. See the [project README](../README.md) for what it
shows and how the pieces fit.

```bash
cp .env.example .env.local   # where the API is
npm install
npm run dev                  # http://localhost:3000 (English), /es (Spanish)
npm test                     # unit tests (Vitest)
npm run lint && npm run typecheck && npm run build
```

- `src/app/`: the English (`/`) and Spanish (`/es`) pages, `/styleguide`, and
  the design tokens in `globals.css`.
- `src/components/dashboard/`: the page and its sections (server components,
  each fetching its own data).
- `src/components/insight/`: InsightFlow's components: lead finding, KPI strip,
  charts, AI analyst, states and controls.
- `src/components/`: base components from Tremor's open-source Dashboard
  template (MIT, see `LICENSE.md`), restyled with the design tokens.
- `src/lib/`: API client, formatting (`format.ts`, the only place numbers
  become text), translations (`i18n.ts`) and data transformations.
