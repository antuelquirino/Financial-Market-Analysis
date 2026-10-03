// Server-side client for the Financial Market API. Screens fetch here, in
// server components, so the browser never talks to the API directly.

import type {
  Period,
  PerformanceResponse,
  RiskResponse,
  SectorsResponse,
  StatusResponse,
  TickersResponse,
} from "./types"

const API_URL = process.env.API_URL ?? "http://localhost:8765"

// The pipeline refreshes once per weekday and the API caches for an hour; ten
// minutes here keeps pages fast while picking up a new close soon after it lands.
const REVALIDATE_SECONDS = 600

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message)
    this.name = "ApiError"
  }
}

type Params = Record<string, string | undefined>

async function get<T>(path: string, params: Params = {}): Promise<T> {
  const url = new URL(path, API_URL)
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) url.searchParams.set(key, value)
  }
  let response: Response
  try {
    response = await fetch(url, { next: { revalidate: REVALIDATE_SECONDS } })
  } catch {
    throw new ApiError("The Financial Market API could not be reached.")
  }
  if (!response.ok) {
    throw new ApiError(
      `The API answered ${response.status} for ${path}.`,
      response.status,
    )
  }
  return (await response.json()) as T
}

// Symbols like ^GSPC need encoding to travel in a path.
const tickerPath = (ticker: string) => encodeURIComponent(ticker)

export const api = {
  status: () => get<StatusResponse>("/status"),
  tickers: () => get<TickersResponse>("/tickers"),
  performance: (ticker: string, period: Period) =>
    get<PerformanceResponse>(`/performance/${tickerPath(ticker)}`, { period }),
  risk: (ticker: string, period: Period) =>
    get<RiskResponse>(`/risk/${tickerPath(ticker)}`, { period }),
  sectors: (period: Period) => get<SectorsResponse>("/sectors", { period }),
}

/**
 * Runs API calls and returns null if any of them fails, so a server component
 * can render its error state without wrapping JSX in try/catch (which would
 * also swallow rendering bugs).
 */
export async function loadOrNull<T>(load: () => Promise<T>): Promise<T | null> {
  try {
    return await load()
  } catch (error) {
    if (error instanceof ApiError) return null
    throw error
  }
}
