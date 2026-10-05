// Every word the dashboard shows, in English and Spanish (Argentina). Findings
// are functions: they receive numbers and dates already formatted for the
// language and put them in a sentence, because word order changes between
// languages. The /styleguide page stays in English.

import type { Locale } from "./locale"
import type { Period } from "./types"

export interface Sentence {
  before: string
  mark: string // the key figure, shown with the marker
  after: string
}

export interface Messages {
  meta: { description: string; risk: string; sectors: string }
  header: {
    subtitle: string
    screens: string
    overview: string
    risk: string
    sectors: string
    language: string
    ticker: string
  }
  freshness: {
    label: (date: string) => string
    late: string
    notUpdated: (missing: number, total: number) => string
  }
  theme: { toDark: string; toLight: string }
  period: {
    label: string
    short: Record<Period, string>
    /** "over the past year" · "since Jan 4, 2021" */
    phrase: (period: Period, start: string) => string
  }
  tickerGroups: { stock: string; sector_etf: string; benchmark: string }
  disclaimer: { title: string; body: string }
  footer: { builtBy: string; designSystem: string }
  states: {
    loading: string
    empty: string
    emptyHint: string
    error: string
    errorHint: string
    retry: string
    retrying: string
    overview: string
    risk: string
    sectors: string
    noVolatility: string
  }
  dateRange: (from: string, to: string) => string
  shorterHistory: (start: string) => string
  kpi: {
    return: string
    cagr: string
    volatility: string
    maxDrawdown: string
    sharpe: string
    beta: string
    losingSessions: string
    cagrNote: string
    annualized: string
    theBenchmark: string
    peakToTrough: string
    perUnitOfRisk: string
    betaNote: (benchmark: string) => string
    ofSessions: (count: string) => string
    same: string
    versus: (benchmark: string) => string
  }
  overview: {
    lead: (ticker: string, ret: string, period: string, versus: string | null) => Sentence
    ahead: (points: string, benchmark: string) => string
    behind: (points: string, benchmark: string) => string
    inLine: (benchmark: string) => string
    noData: (ticker: string) => string
    neverAbove: (ticker: string, end: string) => string
    nearHigh: (ticker: string, end: string) => string
    peaked: (ticker: string, peak: string, date: string, fall: string, end: string) => string
    chartMetric: (range: string, benchmark: string | null) => string
  }
  risk: {
    lead: (ticker: string, period: string, depth: string, peak: string, trough: string, recovered: string | null) => Sentence
    neverFell: (ticker: string, period: string) => Sentence
    noData: (ticker: string) => string
    atHigh: (ticker: string) => string
    belowPeak: (ticker: string, distance: string) => string
    volatilityPeak: (peak: string, date: string, now: string) => string
    notEnoughSessions: string
    distribution: (ticker: string, share: string, worst: string, date: string) => string
    noReturns: (ticker: string) => string
    drawdownMetric: (range: string) => string
    volatilityMetric: string
    distributionMetric: (sessions: string) => string
    oneMonth: string
    oneYear: string
    belowThePeak: (value: string) => string
    binLabel: (lower: string, upper: string) => string
    sessions: (count: number, formatted: string) => string
  }
  sectors: {
    kicker: string
    lead: (sector: string, period: string, ret: string, beat: number, total: number, benchmark: string) => Sentence
    scatterFinding: (best: string, bestSharpe: string, worst: string, worstSharpe: string) => string
    notEnoughData: string
    scatterMetric: (range: string, benchmark: string, highlight: string | null) => string
    highlightSelected: (sector: string, etf: string) => string
    highlightSectorOf: (sector: string, etf: string, ticker: string) => string
    volatilityAxis: string
    annualizedReturn: string
    volatility: string
    sharpe: string
    tableFinding: string
    tableMetric: (benchmark: string, range: string) => string
    tableCaption: string
    columns: {
      sector: string
      total_return: string
      cagr: string
      volatility: string
      max_drawdown: string
      sharpe_ratio: string
      beta: string
      excess_return: string
    }
    benchmarkRow: string
  }
  /** GICS sector names. */
  sectorNames: Record<string, string>
}

const en: Messages = {
  meta: {
    description:
      "Return and risk of US stocks, the 11 SPDR sector ETFs and the S&P 500, refreshed after every US market close.",
    risk: "Risk",
    sectors: "Sectors",
  },
  header: {
    subtitle: "US stocks, sector ETFs and the S&P 500, refreshed after every close",
    screens: "Screens",
    overview: "Overview",
    risk: "Risk",
    sectors: "Sectors",
    language: "Language",
    ticker: "Ticker",
  },
  freshness: {
    label: (date) => `Data updated: ${date} market close`,
    late: "The daily update is late",
    notUpdated: (missing, total) => `${missing} of ${total} tickers not updated`,
  },
  theme: { toDark: "Dark", toLight: "Light" },
  period: {
    label: "Period",
    short: { "1Y": "1Y", "3Y": "3Y", "5Y": "5Y", MAX: "Max" },
    phrase: (period, start) =>
      ({
        "1Y": "over the past year",
        "3Y": "over the past 3 years",
        "5Y": "over the past 5 years",
        MAX: `since ${start}`,
      })[period],
  },
  tickerGroups: { stock: "Stocks", sector_etf: "Sector ETFs", benchmark: "Benchmarks" },
  disclaimer: {
    title: "Not investment advice.",
    body: "For information and education only. Prices come from Yahoo Finance, an unofficial source, adjusted for splits and dividends. Past performance does not predict future returns.",
  },
  footer: { builtBy: "Built by Antuel Quirino", designSystem: "Design system" },
  states: {
    loading: "Loading",
    empty: "No data for this selection.",
    emptyHint: "Try a longer period or another ticker.",
    error: "This chart could not be loaded.",
    errorHint:
      "The data service did not answer. It may be waking up; try again in a few seconds.",
    retry: "Try again",
    retrying: "Retrying…",
    overview: "The overview could not be loaded.",
    risk: "The risk view could not be loaded.",
    sectors: "The sector comparison could not be loaded.",
    noVolatility: "Not enough sessions for rolling volatility.",
  },
  dateRange: (from, to) => `${from} to ${to}`,
  shorterHistory: (start) =>
    `Data starts on ${start}, so this period is shorter than selected.`,
  kpi: {
    return: "Return",
    cagr: "Annualized return",
    volatility: "Volatility",
    maxDrawdown: "Max drawdown",
    sharpe: "Sharpe ratio",
    beta: "Beta",
    losingSessions: "Losing sessions",
    cagrNote: "CAGR",
    annualized: "Annualized",
    theBenchmark: "The benchmark",
    peakToTrough: "Peak to trough",
    perUnitOfRisk: "Return per unit of risk",
    betaNote: (b) => `Moves vs ${b} (1.00 = same)`,
    ofSessions: (count) => `of ${count} sessions`,
    same: "Same",
    versus: (b) => `vs ${b}`,
  },
  overview: {
    lead: (ticker, ret, period, versus) => ({
      before: `${ticker} returned `,
      mark: ret,
      after: versus ? ` ${period}, ${versus}.` : ` ${period}.`,
    }),
    ahead: (points, b) => `${points} ahead of ${b}`,
    behind: (points, b) => `${points} behind ${b}`,
    inLine: (b) => `in line with ${b}`,
    noData: (t) => `${t} has no data for this period`,
    neverAbove: (t, end) => `${t} never rose above its starting price, ending at ${end}`,
    nearHigh: (t, end) => `${t} ended near its high for the period, at ${end}`,
    peaked: (t, peak, date, fall, end) =>
      `${t} peaked at ${peak} on ${date}, then fell ${fall} to ${end}`,
    chartMetric: (range, benchmark) =>
      `Cumulative return from ${range}${benchmark ? `, against ${benchmark}` : ""} · daily closes adjusted for splits and dividends`,
  },
  risk: {
    lead: (t, period, depth, peak, trough, recovered) => ({
      before: `${t}’s deepest fall ${period} was `,
      mark: depth,
      after: `, from ${peak} to ${trough}; ${
        recovered ? `it was back at its high by ${recovered}` : "it has not recovered yet"
      }.`,
    }),
    neverFell: (t, period) => ({
      before: `${t} never fell below a previous high `,
      mark: period,
      after: ".",
    }),
    noData: (t) => `${t} has no data for this period`,
    atHigh: (t) => `${t} ends the period at or near its high`,
    belowPeak: (t, d) => `${t} ends the period ${d} below its peak`,
    volatilityPeak: (peak, date, now) =>
      `Short-term volatility peaked at ${peak} on ${date}; it is ${now} now`,
    notEnoughSessions: "Not enough sessions to measure volatility",
    distribution: (t, share, worst, date) =>
      `${t} fell on ${share} of sessions; its worst day was ${worst} on ${date}`,
    noReturns: (t) => `${t} has no daily returns in this period`,
    drawdownMetric: (range) =>
      `Drawdown: distance below the highest close since the period began · ${range}`,
    volatilityMetric:
      "Annualized volatility of daily returns, over rolling 1-month (21 sessions) and 1-year (252 sessions) windows",
    distributionMetric: (n) => `Daily returns grouped in half-point ranges · ${n} sessions`,
    oneMonth: "1-month",
    oneYear: "1-year",
    belowThePeak: (v) => `${v} below the peak`,
    binLabel: (lower, upper) => `${lower} to ${upper}`,
    sessions: (count, f) => `${f} ${count === 1 ? "session" : "sessions"}`,
  },
  sectors: {
    kicker: "The 11 SPDR sector ETFs",
    lead: (sector, period, ret, beat, total, b) => ({
      before: `${sector} led ${period} with `,
      mark: ret,
      after: `; ${beat} of ${total} sectors beat ${b}.`,
    }),
    scatterFinding: (best, bs, worst, ws) =>
      `${best} earned the most per unit of risk (Sharpe ${bs}); ${worst} the least (${ws})`,
    notEnoughData: "Not enough data to compare risk and return",
    scatterMetric: (range, b, highlight) =>
      `Annualized return against annualized volatility · ${range}. Dashed lines cross at ${b}${highlight ? `; ${highlight}` : ""}`,
    highlightSelected: (sector, etf) => `${sector} (${etf}) is selected`,
    highlightSectorOf: (sector, etf, ticker) => `${sector} (${etf}) is ${ticker}’s sector`,
    volatilityAxis: "Volatility (annualized) →",
    annualizedReturn: "Annualized return",
    volatility: "Volatility",
    sharpe: "Sharpe ratio",
    tableFinding: "Every sector, side by side",
    tableMetric: (b, range) => `Returns, risk and the gap to ${b} · ${range}`,
    tableCaption: "Sector ETFs compared over the period. Select a column header to sort.",
    columns: {
      sector: "Sector",
      total_return: "Return",
      cagr: "Annualized",
      volatility: "Volatility",
      max_drawdown: "Max drawdown",
      sharpe_ratio: "Sharpe",
      beta: "Beta",
      excess_return: "vs benchmark",
    },
    benchmarkRow: "Benchmark",
  },
  sectorNames: {},
}

const es: Messages = {
  meta: {
    description:
      "Rendimiento y riesgo de acciones de EE. UU., los 11 ETF sectoriales SPDR y el S&P 500, actualizados después de cada cierre.",
    risk: "Riesgo",
    sectors: "Sectores",
  },
  header: {
    subtitle: "Acciones de EE. UU., ETF sectoriales y el S&P 500, actualizados después de cada cierre",
    screens: "Pantallas",
    overview: "Resumen",
    risk: "Riesgo",
    sectors: "Sectores",
    language: "Idioma",
    ticker: "Ticker",
  },
  freshness: {
    label: (date) => `Datos al cierre del ${date}`,
    late: "La actualización diaria está atrasada",
    notUpdated: (missing, total) => `${missing} de ${total} tickers sin actualizar`,
  },
  theme: { toDark: "Oscuro", toLight: "Claro" },
  period: {
    label: "Período",
    short: { "1Y": "1A", "3Y": "3A", "5Y": "5A", MAX: "Máx" },
    phrase: (period, start) =>
      ({
        "1Y": "en el último año",
        "3Y": "en los últimos 3 años",
        "5Y": "en los últimos 5 años",
        MAX: `desde el ${start}`,
      })[period],
  },
  tickerGroups: { stock: "Acciones", sector_etf: "ETF sectoriales", benchmark: "Referencias" },
  disclaimer: {
    title: "No es una recomendación de inversión.",
    body: "Solo con fines informativos y educativos. Los precios vienen de Yahoo Finance, una fuente no oficial, ajustados por splits y dividendos. Los rendimientos pasados no anticipan los futuros.",
  },
  footer: { builtBy: "Hecho por Antuel Quirino", designSystem: "Sistema de diseño" },
  states: {
    loading: "Cargando",
    empty: "No hay datos para esta selección.",
    emptyHint: "Probá con un período más largo u otro ticker.",
    error: "No se pudo cargar este gráfico.",
    errorHint:
      "El servicio de datos no respondió. Puede estar arrancando; probá de nuevo en unos segundos.",
    retry: "Reintentar",
    retrying: "Reintentando…",
    overview: "No se pudo cargar el resumen.",
    risk: "No se pudo cargar la vista de riesgo.",
    sectors: "No se pudo cargar la comparación de sectores.",
    noVolatility: "No hay suficientes ruedas para la volatilidad móvil.",
  },
  dateRange: (from, to) => `del ${from} al ${to}`,
  shorterHistory: (start) =>
    `Los datos empiezan el ${start}, así que este período es más corto que el elegido.`,
  kpi: {
    return: "Rendimiento",
    cagr: "Rendimiento anualizado",
    volatility: "Volatilidad",
    maxDrawdown: "Caída máxima",
    sharpe: "Ratio de Sharpe",
    beta: "Beta",
    losingSessions: "Ruedas en baja",
    cagrNote: "Tasa compuesta anual",
    annualized: "Anualizada",
    theBenchmark: "Es la referencia",
    peakToTrough: "De máximo a mínimo",
    perUnitOfRisk: "Rendimiento por unidad de riesgo",
    betaNote: (b) => `Movimiento vs ${b} (1,00 = igual)`,
    ofSessions: (count) => `de ${count} ruedas`,
    same: "Igual",
    versus: (b) => `vs ${b}`,
  },
  overview: {
    lead: (ticker, ret, period, versus) => ({
      before: `${ticker} rindió `,
      mark: ret,
      after: versus ? ` ${period}, ${versus}.` : ` ${period}.`,
    }),
    ahead: (points, b) => `${points} por encima de ${b}`,
    behind: (points, b) => `${points} por debajo de ${b}`,
    inLine: (b) => `en línea con ${b}`,
    noData: (t) => `${t} no tiene datos para este período`,
    neverAbove: (t, end) => `${t} nunca superó su precio inicial y terminó en ${end}`,
    nearHigh: (t, end) => `${t} cerró cerca de su máximo del período, en ${end}`,
    peaked: (t, peak, date, fall, end) =>
      `${t} llegó a ${peak} el ${date} y después cayó ${fall}, hasta ${end}`,
    chartMetric: (range, benchmark) =>
      `Rendimiento acumulado ${range}${benchmark ? `, contra ${benchmark}` : ""} · cierres diarios ajustados por splits y dividendos`,
  },
  risk: {
    lead: (t, period, depth, peak, trough, recovered) => ({
      before: `La peor caída de ${t} ${period} fue de `,
      mark: depth,
      after: `, del ${peak} al ${trough}; ${
        recovered ? `volvió a su máximo el ${recovered}` : "todavía no se recuperó"
      }.`,
    }),
    neverFell: (t, period) => ({
      before: `${t} nunca cayó por debajo de un máximo anterior `,
      mark: period,
      after: ".",
    }),
    noData: (t) => `${t} no tiene datos para este período`,
    atHigh: (t) => `${t} termina el período en su máximo o cerca`,
    belowPeak: (t, d) => `${t} termina el período ${d} por debajo de su máximo`,
    volatilityPeak: (peak, date, now) =>
      `La volatilidad de corto plazo llegó a ${peak} el ${date}; hoy está en ${now}`,
    notEnoughSessions: "No hay suficientes ruedas para medir la volatilidad",
    distribution: (t, share, worst, date) =>
      `${t} bajó en el ${share} de las ruedas; su peor día fue ${worst}, el ${date}`,
    noReturns: (t) => `${t} no tiene rendimientos diarios en este período`,
    drawdownMetric: (range) =>
      `Caída: distancia al cierre más alto desde que empezó el período · ${range}`,
    volatilityMetric:
      "Volatilidad anualizada de los rendimientos diarios, en ventanas móviles de 1 mes (21 ruedas) y 1 año (252 ruedas)",
    distributionMetric: (n) => `Rendimientos diarios agrupados en rangos de medio punto · ${n} ruedas`,
    oneMonth: "1 mes",
    oneYear: "1 año",
    belowThePeak: (v) => `${v} por debajo del máximo`,
    binLabel: (lower, upper) => `de ${lower} a ${upper}`,
    sessions: (count, f) => `${f} ${count === 1 ? "rueda" : "ruedas"}`,
  },
  sectors: {
    kicker: "Los 11 ETF sectoriales SPDR",
    lead: (sector, period, ret, beat, total, b) => ({
      before: `${sector} lideró ${period} con `,
      mark: ret,
      after: `; ${beat} de ${total} sectores le ganaron a ${b}.`,
    }),
    scatterFinding: (best, bs, worst, ws) =>
      `${best} ganó más por unidad de riesgo (Sharpe ${bs}); ${worst}, menos (${ws})`,
    notEnoughData: "No hay datos suficientes para comparar riesgo y rendimiento",
    scatterMetric: (range, b, highlight) =>
      `Rendimiento anualizado contra volatilidad anualizada · ${range}. Las líneas punteadas se cruzan en ${b}${highlight ? `; ${highlight}` : ""}`,
    highlightSelected: (sector, etf) => `${sector} (${etf}) es el elegido`,
    highlightSectorOf: (sector, etf, ticker) => `${sector} (${etf}) es el sector de ${ticker}`,
    volatilityAxis: "Volatilidad (anualizada) →",
    annualizedReturn: "Rendimiento anualizado",
    volatility: "Volatilidad",
    sharpe: "Ratio de Sharpe",
    tableFinding: "Todos los sectores, lado a lado",
    tableMetric: (b, range) => `Rendimiento, riesgo y diferencia con ${b} · ${range}`,
    tableCaption:
      "ETF sectoriales comparados en el período. Elegí el encabezado de una columna para ordenar.",
    columns: {
      sector: "Sector",
      total_return: "Rendimiento",
      cagr: "Anualizado",
      volatility: "Volatilidad",
      max_drawdown: "Caída máx.",
      sharpe_ratio: "Sharpe",
      beta: "Beta",
      excess_return: "vs referencia",
    },
    benchmarkRow: "Referencia",
  },
  sectorNames: {
    "Information Technology": "Tecnología de la información",
    Financials: "Finanzas",
    Energy: "Energía",
    "Health Care": "Salud",
    "Consumer Discretionary": "Consumo discrecional",
    "Consumer Staples": "Consumo básico",
    Industrials: "Industria",
    Materials: "Materiales",
    Utilities: "Servicios públicos",
    "Real Estate": "Inmobiliario",
    "Communication Services": "Servicios de comunicación",
  },
}

export const MESSAGES: Record<Locale, Messages> = { en, es }

/** A GICS sector in the reader's language; English names pass through. */
export function sectorName(sector: string | null | undefined, locale: Locale): string {
  if (!sector) return ""
  return MESSAGES[locale].sectorNames[sector] ?? sector
}
