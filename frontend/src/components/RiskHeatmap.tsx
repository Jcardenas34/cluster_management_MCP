import { useState } from 'react'

export type RiskResults = {
  section: 'raw' | 'excluded'
  attackers: number[]
  defenders: number[]
  win_rates: number[][]   // win_rates[i][j] = defenders[i] vs attackers[j]
  updated_at: string
}

export type RiskHeatmapDisplay = {
  type: 'risk_heatmap'
  section: 'raw' | 'excluded'
  data: Record<'raw' | 'excluded', RiskResults>
}

const SECTION_LABELS: Record<'raw' | 'excluded', string> = {
  raw: 'CEASE-FIRES COUNTED',
  excluded: 'CEASE-FIRES EXCLUDED',
}

// Diverging scale centred on 50%: defender-favoured (red) → even (gray) → attacker-favoured (blue)
const DEFENDER = [0xe6, 0x67, 0x67]
const NEUTRAL  = [0x38, 0x38, 0x35]
const ATTACKER = [0x39, 0x87, 0xe5]

function mix(a: number[], b: number[], t: number): string {
  const c = a.map((v, i) => Math.round(v + (b[i] - v) * t))
  return `rgb(${c[0]}, ${c[1]}, ${c[2]})`
}

function winRateColor(pct: number): string {
  const p = Math.min(100, Math.max(0, pct))
  return p < 50 ? mix(DEFENDER, NEUTRAL, p / 50) : mix(NEUTRAL, ATTACKER, (p - 50) / 50)
}

// Above this many columns, values move to the hover readout only
const MAX_LABELLED_COLUMNS = 12

type Hovered = { attackers: number; defenders: number; value: number } | null

export default function RiskHeatmap({ display }: { display: RiskHeatmapDisplay }) {
  const [section, setSection] = useState(display.section)
  const [hovered, setHovered] = useState<Hovered>(null)
  const results = display.data[section]
  const showValues = results.attackers.length <= MAX_LABELLED_COLUMNS

  return (
    <div className="risk-heatmap">
      <div className="risk-heatmap-header">
        <span className="risk-heatmap-title">ATTACKER WIN RATE (%)</span>
        <div className="risk-heatmap-tabs">
          {(['raw', 'excluded'] as const).map(s => (
            <button
              key={s}
              className={`risk-heatmap-tab ${s === section ? 'active' : ''}`}
              onClick={() => setSection(s)}
            >{SECTION_LABELS[s]}</button>
          ))}
        </div>
      </div>

      <div className="risk-heatmap-scroll">
        <div
          className="risk-heatmap-grid"
          style={{ gridTemplateColumns: `auto repeat(${results.attackers.length}, minmax(${showValues ? 38 : 18}px, 1fr))` }}
          onMouseLeave={() => setHovered(null)}
        >
          <div className="risk-heatmap-corner">DEF ╲ ATT</div>
          {results.attackers.map(a => <div key={`a${a}`} className="risk-heatmap-axis col">{a}</div>)}

          {results.defenders.map((d, i) => (
            <div key={`row${d}`} style={{ display: 'contents' }}>
              <div className="risk-heatmap-axis row">{d}</div>
              {results.attackers.map((a, j) => {
                const value = results.win_rates[i][j]
                const isHovered = hovered?.attackers === a && hovered?.defenders === d
                return (
                  <div
                    key={`c${d}-${a}`}
                    className={`risk-heatmap-cell ${isHovered ? 'hovered' : ''}`}
                    style={{ background: winRateColor(value) }}
                    title={`${a} attackers vs ${d} defenders: ${value}%`}
                    aria-label={`${a} attackers vs ${d} defenders: ${value}% attacker win rate`}
                    onMouseEnter={() => setHovered({ attackers: a, defenders: d, value })}
                  >
                    {showValues ? value.toFixed(0) : null}
                  </div>
                )
              })}
            </div>
          ))}
        </div>
      </div>

      <div className="risk-heatmap-readout">
        {hovered
          ? <><strong>{hovered.value.toFixed(2)}%</strong> attacker win · {hovered.attackers} attackers vs {hovered.defenders} defenders</>
          : <span className="risk-heatmap-muted">hover a cell for the exact win rate</span>}
      </div>

      <div className="risk-heatmap-legend">
        <span>DEFENDER FAVOURED</span>
        <div className="risk-heatmap-ramp">
          <div
            className="risk-heatmap-ramp-bar"
            style={{ background: `linear-gradient(to right, ${winRateColor(0)}, ${winRateColor(50)}, ${winRateColor(100)})` }}
          />
          <div className="risk-heatmap-ramp-ticks"><span>0</span><span>50</span><span>100</span></div>
        </div>
        <span>ATTACKER FAVOURED</span>
      </div>

      <div className="risk-heatmap-muted risk-heatmap-updated">
        last run {new Date(results.updated_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
      </div>
    </div>
  )
}
