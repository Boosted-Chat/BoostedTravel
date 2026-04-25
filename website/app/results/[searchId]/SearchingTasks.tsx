'use client'

import { useEffect, useMemo, useState } from 'react'
import { useTranslations } from 'next-intl'

interface Props {
  originLabel?: string
  originCode?: string
  destinationLabel?: string
  destinationCode?: string
  progress?: { checked: number; total: number; found: number }
  searchedAt?: string
}

const SAMPLE_AIRLINES = [
  'ryanair.com', 'easyjet.com', 'wizzair.com', 'norwegian.com',
  'vueling.com', 'skyscanner.com', 'kayak.com', 'transavia.com',
  'britishairways.com', 'iberia.com', 'airfrance.com', 'klm.com',
  'momondo.com', 'spirit.com', 'flydubai.com', 'airasia.com',
  'indigo.co.in', 'southwest.com', 'jetblue.com', 'latam.com',
  'volotea.com', 'wizz.com', 'corendon.com', 'sunexpress.com',
]

type StepState = 'done' | 'active' | 'pending'

// ── Landmark buildings ─────────────────────────────────────────────────────────
// All designed for viewBox 0 0 180 86 with ground at y=76, growing upward.

function House({ x }: { x: number }) {
  // 34px wide · chimney top y=35 · roof peak y=43 · body y=57–76
  return (
    <g>
      {/* Chimney — drawn before roof so roof overlaps its base */}
      <rect x={x + 24} y={37} width={5} height={20} rx={1} className="st-building" />
      <rect x={x + 23} y={35} width={7} height={3} rx={0.5} className="st-building" />
      {/* Gabled roof */}
      <path d={`M${x - 1} 57 L${x + 17} 43 L${x + 35} 57 Z`} className="st-building" />
      {/* Body */}
      <rect x={x} y={57} width={34} height={19} className="st-building" />
      {/* Arched door */}
      <path d={`M${x + 13} 76 L${x + 13} 65 Q${x + 17} 60 ${x + 21} 65 L${x + 21} 76 Z`} className="st-window" />
      {/* Windows */}
      <rect x={x + 2} y={60} width={9} height={7} rx={1} className="st-window" />
      <rect x={x + 23} y={60} width={9} height={7} rx={1} className="st-window" />
    </g>
  )
}

function BigBen({ x }: { x: number }) {
  // 18px wide (x−1…x+17) · spire top y=12
  return (
    <g>
      {/* Base steps */}
      <rect x={x - 1} y={72} width={18} height={4} className="st-building" />
      <rect x={x} y={66} width={16} height={7} className="st-building" />
      {/* Main tower shaft */}
      <rect x={x + 2} y={33} width={12} height={36} className="st-building" />
      {/* Belfry (slightly wider) */}
      <rect x={x} y={25} width={16} height={10} className="st-building" />
      {/* Clock face */}
      <circle cx={x + 8} cy={30} r={4} className="st-window" />
      {/* Gothic spire */}
      <path d={`M${x + 2} 25 L${x + 8} 12 L${x + 14} 25 Z`} className="st-building" />
      {/* Flanking pinnacles */}
      <path d={`M${x + 1} 25 L${x + 4} 20 L${x + 7} 25 Z`} className="st-building" />
      <path d={`M${x + 9} 25 L${x + 12} 20 L${x + 15} 25 Z`} className="st-building" />
    </g>
  )
}

function Skyscraper({ x }: { x: number }) {
  // 22px wide · antenna top y=8
  const rows = [18, 26, 34, 42, 50, 58]
  const cols = [4, 10, 16]
  return (
    <g>
      {/* Podium */}
      <rect x={x} y={66} width={22} height={10} rx={1} className="st-building" />
      {/* Tower */}
      <rect x={x + 2} y={18} width={18} height={50} className="st-building" />
      {/* Cap */}
      <rect x={x + 4} y={14} width={14} height={6} className="st-building" />
      {/* Antenna */}
      <rect x={x + 10} y={8} width={2} height={8} className="st-building" />
      {/* Window grid */}
      {rows.flatMap(wy => cols.map(wx => (
        <rect key={`${wx}-${wy}`} x={x + wx} y={wy} width={4} height={5} rx={0.5} className="st-window" />
      )))}
    </g>
  )
}

function EmpireState({ x }: { x: number }) {
  // 24px wide · antenna top y=9 · Art Deco setbacks
  return (
    <g>
      <rect x={x} y={60} width={24} height={16} className="st-building" />
      <rect x={x + 2} y={52} width={20} height={10} className="st-building" />
      <rect x={x + 4} y={44} width={16} height={10} className="st-building" />
      <rect x={x + 6} y={36} width={12} height={10} className="st-building" />
      <rect x={x + 7} y={28} width={10} height={10} className="st-building" />
      <rect x={x + 9} y={22} width={6} height={8} className="st-building" />
      {/* Mooring mast antenna */}
      <rect x={x + 11} y={9} width={2} height={15} className="st-building" />
      {/* Base windows */}
      {[62, 68].flatMap(wy => [2, 8, 16].map(wx => (
        <rect key={`${wx}-${wy}`} x={x + wx} y={wy} width={3} height={4} rx={0.5} className="st-window" />
      )))}
    </g>
  )
}

function Cathedral({ x }: { x: number }) {
  // 46px wide · twin spire tops y=18
  return (
    <g>
      {/* Central nave */}
      <rect x={x + 10} y={46} width={26} height={30} className="st-building" />
      {/* Bell towers */}
      <rect x={x} y={30} width={12} height={46} className="st-building" />
      <rect x={x + 34} y={30} width={12} height={46} className="st-building" />
      {/* Gothic spires */}
      <path d={`M${x + 1} 30 L${x + 6} 18 L${x + 11} 30 Z`} className="st-building" />
      <path d={`M${x + 35} 30 L${x + 40} 18 L${x + 45} 30 Z`} className="st-building" />
      {/* Rose window */}
      <circle cx={x + 23} cy={53} r={5} className="st-window" />
      {/* Gothic arch doorway */}
      <path d={`M${x + 14} 76 L${x + 14} 70 Q${x + 23} 64 ${x + 32} 70 L${x + 32} 76 Z`} className="st-window" />
      {/* Tower windows */}
      <rect x={x + 2} y={36} width={8} height={9} rx={1} className="st-window" />
      <rect x={x + 36} y={36} width={8} height={9} rx={1} className="st-window" />
    </g>
  )
}

function Colosseum({ x }: { x: number }) {
  // 70px wide · wide low oval amphitheatre · 3 tiers
  // Attic y=44-53, 2nd tier y=53-63, Ground y=63-76
  return (
    <g>
      {/* Attic story (narrowest — suggests oval tapering) */}
      <rect x={x + 5} y={44} width={60} height={9} className="st-building" />
      {/* Second story */}
      <rect x={x + 2} y={53} width={66} height={10} className="st-building" />
      {/* Ground story */}
      <rect x={x} y={63} width={70} height={13} rx={1} className="st-building" />
      {/* Ground arches × 6 */}
      {[0,1,2,3,4,5].map(i => (
        <path key={i}
          d={`M${x+4+i*11} 76 L${x+4+i*11} 68 Q${x+8.5+i*11} 63 ${x+13+i*11} 68 L${x+13+i*11} 76 Z`}
          className="st-window" />
      ))}
      {/* Second tier arches × 5 */}
      {[0,1,2,3,4].map(i => (
        <path key={i}
          d={`M${x+5+i*12} 63 L${x+5+i*12} 57 Q${x+9+i*12} 53 ${x+13+i*12} 57 L${x+13+i*12} 63 Z`}
          className="st-window" />
      ))}
      {/* Attic arches × 4 */}
      {[0,1,2,3].map(i => (
        <path key={i}
          d={`M${x+8+i*14} 53 L${x+8+i*14} 48 Q${x+13+i*14} 44 ${x+18+i*14} 48 L${x+18+i*14} 53 Z`}
          className="st-window" />
      ))}
    </g>
  )
}

function GoldenGateTower({ x }: { x: number }) {
  // 16px wide · bridge pylon · top y=10
  return (
    <g>
      {/* Two vertical legs */}
      <rect x={x} y={34} width={5} height={42} className="st-building" />
      <rect x={x + 11} y={34} width={5} height={42} className="st-building" />
      {/* Portal crossbeam */}
      <rect x={x} y={28} width={16} height={8} rx={1} className="st-building" />
      {/* Tapering upper sections */}
      <rect x={x + 2} y={18} width={12} height={12} className="st-building" />
      <rect x={x + 3} y={14} width={10} height={6} className="st-building" />
      <rect x={x + 5} y={10} width={6} height={6} rx={1} className="st-building" />
      {/* Cable anchor cross-braces */}
      <rect x={x} y={42} width={16} height={2} className="st-window" />
      <rect x={x} y={52} width={16} height={2} className="st-window" />
      <rect x={x} y={62} width={16} height={2} className="st-window" />
    </g>
  )
}
function Villa({ x }: { x: number }) {
  // 30px wide · Mediterranean flat-roof apartment · ground at y=76
  return (
    <g>
      {/* Flat parapet (slightly wider for rooftop overhang effect) */}
      <rect x={x - 1} y={40} width={32} height={5} rx={1} className="st-building" />
      {/* Body (inset under parapet) */}
      <rect x={x + 1} y={44} width={28} height={32} className="st-building" />
      {/* Upper floor: two arched windows */}
      <path d={`M${x+3} 63 L${x+3} 55 Q${x+7} 50 ${x+11} 55 L${x+11} 63 Z`} className="st-window" />
      <path d={`M${x+19} 63 L${x+19} 55 Q${x+23} 50 ${x+27} 55 L${x+27} 63 Z`} className="st-window" />
      {/* Ground floor: rect windows flanking arched doorway */}
      <rect x={x+3} y={66} width={6} height={5} rx={1} className="st-window" />
      <rect x={x+21} y={66} width={6} height={5} rx={1} className="st-window" />
      <path d={`M${x+12} 76 L${x+12} 70 Q${x+15} 66 ${x+18} 70 L${x+18} 76 Z`} className="st-window" />
    </g>
  )
}

function EiffelTower({ x }: { x: number }) {
  // 16px wide · Eiffel Tower · antenna top y=10
  const cx = x + 8
  return (
    <g>
      <path d={`M${x} 76 L${cx-3} 58 L${cx-1} 58 L${x+4} 76Z`} className="st-building" />
      <path d={`M${x+16} 76 L${cx+3} 58 L${cx+1} 58 L${x+12} 76Z`} className="st-building" />
      <rect x={x+3} y={68} width={10} height={1.5} rx={0.5} className="st-building" />
      <rect x={x+3} y={56} width={10} height={2} rx={1} className="st-building" />
      <path d={`M${cx-3} 58 L${cx-2} 40 L${cx+2} 40 L${cx+3} 58Z`} className="st-building" />
      <rect x={x+4} y={38} width={8} height={2} rx={1} className="st-building" />
      <path d={`M${cx-2} 40 L${cx} 22 L${cx+2} 40Z`} className="st-building" />
      <rect x={cx-0.5} y={10} width={1} height={12} className="st-building" />
    </g>
  )
}

function Sagrada({ x }: { x: number }) {
  // 36px wide · Sagrada Família · tallest spire top y=10
  return (
    <g>
      <rect x={x+4} y={46} width={28} height={30} className="st-building" />
      {/* Outer shorter spires */}
      <path d={`M${x+1} 52 L${x+4} 28 L${x+7} 52Z`} className="st-building" />
      <path d={`M${x+29} 52 L${x+32} 28 L${x+35} 52Z`} className="st-building" />
      {/* Inner taller spires */}
      <path d={`M${x+10} 46 L${x+13} 14 L${x+16} 46Z`} className="st-building" />
      <path d={`M${x+20} 46 L${x+23} 10 L${x+26} 46Z`} className="st-building" />
      <circle cx={x+18} cy={55} r={4.5} className="st-window" />
      <path d={`M${x+7} 76 L${x+7} 68 Q${x+11} 63 ${x+15} 68 L${x+15} 76Z`} className="st-window" />
      <path d={`M${x+21} 76 L${x+21} 68 Q${x+25} 63 ${x+29} 68 L${x+29} 76Z`} className="st-window" />
    </g>
  )
}

function PalaceOfCulture({ x }: { x: number }) {
  // 22px wide · Warsaw Palace of Culture · spire top y=11
  return (
    <g>
      <rect x={x} y={58} width={22} height={18} className="st-building" />
      <rect x={x+2} y={48} width={18} height={12} className="st-building" />
      <rect x={x+4} y={38} width={14} height={12} className="st-building" />
      <rect x={x+6} y={30} width={10} height={10} className="st-building" />
      <rect x={x+8} y={22} width={6} height={10} className="st-building" />
      <path d={`M${x+8} 22 L${x+11} 11 L${x+14} 22Z`} className="st-building" />
      <rect x={x+2} y={62} width={4} height={5} rx={0.5} className="st-window" />
      <rect x={x+9} y={62} width={4} height={5} rx={0.5} className="st-window" />
      <rect x={x+16} y={62} width={4} height={5} rx={0.5} className="st-window" />
      <rect x={x+4} y={50} width={2} height={6} rx={0.5} className="st-window" />
      <rect x={x+16} y={50} width={2} height={6} rx={0.5} className="st-window" />
    </g>
  )
}

function Burj({ x }: { x: number }) {
  // 10px wide · Burj Khalifa · needle top y=8
  return (
    <g>
      <rect x={x} y={66} width={10} height={10} className="st-building" />
      <path d={`M${x} 66 L${x+1} 48 L${x+9} 48 L${x+10} 66Z`} className="st-building" />
      <path d={`M${x+1} 48 L${x+2} 34 L${x+8} 34 L${x+9} 48Z`} className="st-building" />
      <path d={`M${x+2} 34 L${x+3} 24 L${x+7} 24 L${x+8} 34Z`} className="st-building" />
      <path d={`M${x+3} 24 L${x+4} 16 L${x+6} 16 L${x+7} 24Z`} className="st-building" />
      <path d={`M${x+4} 16 L${x+5} 8 L${x+6} 16Z`} className="st-building" />
    </g>
  )
}

function CNTower({ x }: { x: number }) {
  // 12px wide · CN Tower · antenna top y=9
  const cx = x + 6
  return (
    <g>
      <path d={`M${x} 76 L${cx-2} 46 L${cx+2} 46 L${x+12} 76Z`} className="st-building" />
      <rect x={cx-2} y={32} width={4} height={14} className="st-building" />
      <ellipse cx={cx} cy={30} rx={7} ry={4.5} className="st-building" />
      <ellipse cx={cx} cy={29} rx={5} ry={2} className="st-window" />
      <rect x={cx-0.75} y={9} width={1.5} height={21} className="st-building" />
    </g>
  )
}

function WashingtonMonument({ x }: { x: number }) {
  // 10px wide · obelisk · pyramidion top y=12
  return (
    <g>
      <path d={`M${x} 76 L${x+1} 20 L${x+9} 20 L${x+10} 76Z`} className="st-building" />
      <path d={`M${x+1} 20 L${x+5} 12 L${x+9} 20Z`} className="st-building" />
    </g>
  )
}

function SydneyOpera({ x }: { x: number }) {
  // 44px wide · shell roofs on platform · tallest shell top y=28
  return (
    <g>
      <rect x={x} y={68} width={44} height={8} rx={1} className="st-building" />
      <path d={`M${x+2} 68 Q${x+4} 38 ${x+18} 28 Q${x+22} 68 ${x+22} 68Z`} className="st-building" />
      <path d={`M${x+5} 68 Q${x+7} 46 ${x+16} 38 Q${x+19} 68 ${x+19} 68Z`} className="st-window" />
      <path d={`M${x+23} 68 Q${x+25} 42 ${x+37} 34 Q${x+40} 68 ${x+40} 68Z`} className="st-building" />
      <path d={`M${x+26} 68 Q${x+28} 48 ${x+36} 42 Q${x+38} 68 ${x+38} 68Z`} className="st-window" />
      <path d={`M${x+40} 68 Q${x+42} 54 ${x+44} 49 L${x+44} 68Z`} className="st-building" />
    </g>
  )
}

function SaintBasils({ x }: { x: number }) {
  // 40px wide · St Basil's Cathedral · central spire top y=14
  return (
    <g>
      <rect x={x+6} y={54} width={28} height={22} className="st-building" />
      <rect x={x+16} y={40} width={8} height={16} className="st-building" />
      <path d={`M${x+16} 40 Q${x+14} 28 ${x+20} 22 Q${x+26} 28 ${x+24} 40Z`} className="st-building" />
      <path d={`M${x+18} 22 L${x+20} 14 L${x+22} 22Z`} className="st-building" />
      <rect x={x+1} y={56} width={7} height={20} className="st-building" />
      <path d={`M${x+1} 56 Q${x} 48 ${x+4.5} 44 Q${x+9} 48 ${x+8} 56Z`} className="st-building" />
      <rect x={x+32} y={56} width={7} height={20} className="st-building" />
      <path d={`M${x+32} 56 Q${x+31} 48 ${x+35.5} 44 Q${x+40} 48 ${x+39} 56Z`} className="st-building" />
      <rect x={x+9} y={50} width={6} height={26} className="st-building" />
      <path d={`M${x+9} 50 Q${x+8} 43 ${x+12} 40 Q${x+16} 43 ${x+15} 50Z`} className="st-building" />
      <rect x={x+25} y={50} width={6} height={26} className="st-building" />
      <path d={`M${x+25} 50 Q${x+24} 43 ${x+28} 40 Q${x+32} 43 ${x+31} 50Z`} className="st-building" />
    </g>
  )
}

function BrandenburgGate({ x }: { x: number }) {
  // 34px wide · Brandenburg Gate · quadriga top y=32
  return (
    <g>
      <rect x={x} y={42} width={34} height={8} className="st-building" />
      <path d={`M${x+8} 42 Q${x+17} 32 ${x+26} 42Z`} className="st-building" />
      <rect x={x} y={49} width={34} height={2} className="st-building" />
      {[0,7,14,21,28].map(i => (
        <rect key={i} x={x+i+1} y={51} width={4} height={21} rx={0.5} className="st-building" />
      ))}
      <rect x={x} y={72} width={34} height={4} className="st-building" />
    </g>
  )
}

function Parthenon({ x }: { x: number }) {
  // 44px wide · Parthenon · pediment peak y=30
  return (
    <g>
      <path d={`M${x} 46 L${x+22} 30 L${x+44} 46Z`} className="st-building" />
      <rect x={x} y={46} width={44} height={4} className="st-building" />
      {[0,5.5,11,16.5,22,27.5,33,38.5].map((cx, i) => (
        <rect key={i} x={x+cx+0.5} y={50} width={3.5} height={20} rx={0.5} className="st-building" />
      ))}
      <rect x={x} y={70} width={44} height={2} className="st-building" />
      <rect x={x+1} y={72} width={42} height={2} className="st-building" />
      <rect x={x+1} y={74} width={42} height={2} className="st-building" />
    </g>
  )
}

function TokyoTower({ x }: { x: number }) {
  // 20px wide · Tokyo Tower · needle top y=9
  const cx = x + 10
  return (
    <g>
      <path d={`M${x} 76 L${cx-4} 54 L${cx-2} 54 L${x+4} 76Z`} className="st-building" />
      <path d={`M${x+20} 76 L${cx+4} 54 L${cx+2} 54 L${x+16} 76Z`} className="st-building" />
      <rect x={x+3} y={65} width={14} height={1.5} rx={0.5} className="st-building" />
      <rect x={cx-6} y={52} width={12} height={3} rx={0.5} className="st-building" />
      <path d={`M${cx-4} 55 L${cx-2} 38 L${cx+2} 38 L${cx+4} 55Z`} className="st-building" />
      <rect x={cx-3} y={46} width={6} height={1.5} rx={0.5} className="st-building" />
      <rect x={cx-4} y={36} width={8} height={3} rx={0.5} className="st-building" />
      <path d={`M${cx-2} 39 L${cx-1} 20 L${cx+1} 20 L${cx+2} 39Z`} className="st-building" />
      <rect x={cx-0.5} y={9} width={1} height={11} className="st-building" />
    </g>
  )
}

function Pyramid({ x }: { x: number }) {
  // 38px wide · Egyptian pyramid · peak y=18
  return (
    <g>
      <path d={`M${x} 76 L${x+19} 18 L${x+38} 76Z`} className="st-building" />
      <rect x={x+15} y={68} width={8} height={8} className="st-window" />
    </g>
  )
}

function PetrasTowers({ x }: { x: number }) {
  // 28px wide · Petronas Towers · spire tops y=10
  return (
    <g>
      <rect x={x} y={34} width={11} height={42} className="st-building" />
      <rect x={x+1} y={28} width={9} height={8} className="st-building" />
      <rect x={x+2} y={22} width={7} height={8} className="st-building" />
      <rect x={x+4} y={10} width={3} height={14} className="st-building" />
      <rect x={x+17} y={34} width={11} height={42} className="st-building" />
      <rect x={x+18} y={28} width={9} height={8} className="st-building" />
      <rect x={x+19} y={22} width={7} height={8} className="st-building" />
      <rect x={x+21} y={10} width={3} height={14} className="st-building" />
      <rect x={x+11} y={46} width={6} height={4} rx={1} className="st-building" />
      <rect x={x+2} y={38} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+6} y={38} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+2} y={46} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+6} y={46} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+19} y={38} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+23} y={38} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+19} y={46} width={3} height={4} rx={0.5} className="st-window" />
      <rect x={x+23} y={46} width={3} height={4} rx={0.5} className="st-window" />
    </g>
  )
}

function Townhall({ x }: { x: number }) {
  // 30px wide · civic town hall with clock cupola · fits any city
  return (
    <g>
      {/* Steps */}
      <rect x={x-1} y={73} width={32} height={3} rx={0.5} className="st-building" />
      {/* Main body */}
      <rect x={x+1} y={50} width={28} height={23} className="st-building" />
      {/* Cornice */}
      <rect x={x} y={48} width={30} height={3} rx={0.5} className="st-building" />
      {/* Pediment */}
      <path d={`M${x+3} 48 L${x+15} 38 L${x+27} 48Z`} className="st-building" />
      {/* Central tower shaft */}
      <rect x={x+11} y={22} width={8} height={28} className="st-building" />
      {/* Clock face */}
      <circle cx={x+15} cy={33} r={3.5} className="st-window" />
      {/* Cupola */}
      <rect x={x+12} y={16} width={6} height={7} className="st-building" />
      <path d={`M${x+12} 16 Q${x+15} 10 ${x+18} 16Z`} className="st-building" />
      {/* Columns (4) */}
      {[3,9,16,22].map(cx => (
        <rect key={cx} x={x+cx} y={50} width={3} height={21} rx={0.5} className="st-window" />
      ))}
      {/* Front door */}
      <path d={`M${x+12} 76 L${x+12} 67 Q${x+15} 63 ${x+18} 67 L${x+18} 76Z`} className="st-window" />
    </g>
  )
}

function ApartmentBlock({ x }: { x: number }) {
  // 26px wide · generic mid-rise apartment with balconies · fits any city
  const rows = [26, 35, 44, 53, 62]
  return (
    <g>
      {/* Body */}
      <rect x={x} y={22} width={26} height={54} className="st-building" />
      {/* Rooftop parapet */}
      <rect x={x-1} y={20} width={28} height={3} rx={0.5} className="st-building" />
      {/* Windows grid — 3 cols, 5 rows */}
      {rows.flatMap(wy => [2, 10, 18].map(wx => (
        <rect key={`${wx}-${wy}`} x={x+wx} y={wy} width={6} height={5} rx={0.5} className="st-window" />
      )))}
      {/* Balcony ledges (rows 2, 3, 4) */}
      {[35, 44, 53].map(wy => (
        <rect key={wy} x={x-1} y={wy+5} width={28} height={1.5} rx={0.5} className="st-building" />
      ))}
      {/* Entrance */}
      <rect x={x+9} y={65} width={8} height={11} className="st-window" />
    </g>
  )
}

// ── Seed helpers ──────────────────────────────────────────────────────────────
function hashStr(s: string): number {
  // FNV-1a 32-bit — deterministic, no external deps
  let h = 2166136261
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619) >>> 0
  }
  return h
}

function seededShuffle<T>(arr: T[], seed: number): T[] {
  const out = [...arr]
  let s = seed
  for (let i = out.length - 1; i > 0; i--) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0
    const j = s % (i + 1)
    ;[out[i], out[j]] = [out[j], out[i]]
  }
  return out
}

// ── Landmark pool ──────────────────────────────────────────────────────────────
type LandmarkDef = { id: string; width: number; render: (x: number) => React.ReactNode }

const LANDMARKS: LandmarkDef[] = [
  // ── Generic pool (rotate into any city) ──────────────────────────────────
  { id: 'house',           width: 36, render: x => <House x={x} /> },
  { id: 'skyscraper',      width: 24, render: x => <Skyscraper x={x} /> },
  { id: 'cathedral',       width: 48, render: x => <Cathedral x={x} /> },
  { id: 'villa',           width: 32, render: x => <Villa x={x} /> },
  { id: 'townhall',        width: 32, render: x => <Townhall x={x} /> },
  { id: 'apartment',       width: 28, render: x => <ApartmentBlock x={x} /> },
  // ── City-specific heroes ──────────────────────────────────────────────────
  { id: 'bigben',          width: 20, render: x => <BigBen x={x} /> },
  { id: 'empire',          width: 26, render: x => <EmpireState x={x} /> },
  { id: 'colosseum',       width: 72, render: x => <Colosseum x={x} /> },
  { id: 'goldengate',      width: 18, render: x => <GoldenGateTower x={x} /> },
  { id: 'eiffeltower',     width: 18, render: x => <EiffelTower x={x} /> },
  { id: 'sagrada',         width: 38, render: x => <Sagrada x={x} /> },
  { id: 'palaceofculture', width: 24, render: x => <PalaceOfCulture x={x} /> },
  { id: 'burj',            width: 12, render: x => <Burj x={x} /> },
  { id: 'cntower',         width: 14, render: x => <CNTower x={x} /> },
  { id: 'washingtonmon',   width: 12, render: x => <WashingtonMonument x={x} /> },
  { id: 'sydneyopera',     width: 46, render: x => <SydneyOpera x={x} /> },
  { id: 'saintbasils',     width: 42, render: x => <SaintBasils x={x} /> },
  { id: 'brandenburggate', width: 36, render: x => <BrandenburgGate x={x} /> },
  { id: 'parthenon',       width: 46, render: x => <Parthenon x={x} /> },
  { id: 'tokyotower',      width: 22, render: x => <TokyoTower x={x} /> },
  { id: 'pyramid',         width: 40, render: x => <Pyramid x={x} /> },
  { id: 'petrastowers',    width: 30, render: x => <PetrasTowers x={x} /> },
]

const CITY_HEROES: Record<string, string[]> = {
  // UK — London
  LON: ['bigben'], LHR: ['bigben'], LGW: ['bigben'], STN: ['bigben'], LCY: ['bigben'], LTN: ['bigben'],
  // USA — New York
  NYC: ['empire'], JFK: ['empire'], LGA: ['empire'], EWR: ['empire'],
  // Italy — Rome
  ROM: ['colosseum'], FCO: ['colosseum'], CIA: ['colosseum'],
  // USA — San Francisco
  SFO: ['goldengate'],
  // France — Paris
  PAR: ['eiffeltower'], CDG: ['eiffeltower'], ORY: ['eiffeltower'], LBG: ['eiffeltower'],
  // Spain — Barcelona
  BCN: ['sagrada'],
  // Poland — Warsaw
  WAW: ['palaceofculture'], WMI: ['palaceofculture'],
  // UAE — Dubai
  DXB: ['burj'], DWC: ['burj'],
  // Canada — Toronto
  YYZ: ['cntower'], YTZ: ['cntower'],
  // USA — Washington DC
  DCA: ['washingtonmon'], IAD: ['washingtonmon'], BWI: ['washingtonmon'],
  // Australia — Sydney
  SYD: ['sydneyopera'],
  // Russia — Moscow
  SVO: ['saintbasils'], DME: ['saintbasils'], VKO: ['saintbasils'],
  // Germany — Berlin
  BER: ['brandenburggate'], TXL: ['brandenburggate'],
  // Greece — Athens
  ATH: ['parthenon'],
  // Japan — Tokyo
  TYO: ['tokyotower'], NRT: ['tokyotower'], HND: ['tokyotower'],
  // Egypt — Cairo
  CAI: ['pyramid'],
  // Malaysia — Kuala Lumpur
  KUL: ['petrastowers'],
}

function Skyline({ mirrored = false, seed = 0, cityCode = '' }: { mirrored?: boolean; seed?: number; cityCode?: string }) {
  const heroIds = CITY_HEROES[cityCode.toUpperCase()] ?? []
  const heroes = heroIds.map(id => LANDMARKS.find(l => l.id === id)!).filter(Boolean)

  // Generic pool: exclude hero landmarks pinned to ANY city (bigben/empire/colosseum/goldengate
  // should not randomly appear for a city they don't belong to)
  const citySpecificIds = new Set(Object.values(CITY_HEROES).flat())
  const genericPool = LANDMARKS.filter(l => !citySpecificIds.has(l.id))

  // Shuffle generics deterministically
  const shuffledGenerics = seededShuffle(genericPool, seed ^ (mirrored ? 0xdeadbeef : 0))

  // Greedily pack: heroes first, then fill with shuffled generics
  const gap = 6
  const maxX = 172
  const selected: Array<{ def: LandmarkDef; x: number }> = []
  let curX = 5
  for (const def of [...heroes, ...shuffledGenerics]) {
    if (curX + def.width > maxX) break
    selected.push({ def, x: curX })
    curX += def.width + gap
  }

  return (
    <svg className={`st-skyline${mirrored ? ' st-skyline--mirrored' : ''}`} viewBox="0 0 180 86" fill="none" aria-hidden="true">
      {/* Buildings */}
      {selected.map(({ def, x }) => (
        <g key={def.id}>{def.render(x)}</g>
      ))}
      {/* Ground line */}
      <rect className="st-skyline-ground" x="0" y="78" width="180" height="5" rx="2.5" fill="currentColor" />
    </svg>
  )
}

function FlightArc() {
  const visiblePath = 'M20 140C120 56 240 30 360 30C480 30 600 56 700 140'
  // Lead-in: approach (20,140) from off-canvas — shorter (y=200 not y=240), same tangent angle preserved
  // ctrl2=(-30,182): direction to (20,140) is (50,-42) ≈ same unit vector as original (100,-84) ✓
  // Lead-out: mirror — ctrl1=(750,182), end=(750,200)
  const motionPath = 'M-30 200 C-30 200 -30 182 20 140 C120 56 240 30 360 30 C480 30 600 56 700 140 C750 182 750 200 750 200'

  return (
    <svg className="st-arc" viewBox="0 0 720 168" fill="none" aria-hidden="true" style={{ overflow: 'visible' }}>
      <defs>
        {/* Mask punches a transparent hole in the arc line wherever the plane is */}
        <mask id="arcLineMask">
          {/* White = visible; oversized rect covers entire visible+overflow area */}
          <rect x="-200" y="-200" width="1120" height="568" fill="white" />
          {/* Black circle = invisible (cuts out the line) — follows exact same path as plane */}
          <circle r="38" fill="black">
            <animateMotion dur="5.8s" repeatCount="indefinite" path={motionPath} />
          </circle>
        </mask>
        {/* Clip plane at viewBox bottom (y=168) so it disappears cleanly on entry/exit */}
      </defs>
      <path d={visiblePath} className="st-arc-line" pathLength="1" mask="url(#arcLineMask)" />
      <g className="st-arc-plane-group">
        {/* User plane SVG — 24×24 viewBox, default 45° NE → rotate(45) so it points east for rotate="auto" */}
        <g transform="rotate(45) scale(2.2) translate(-12, -12)">
          <path
            className="st-arc-plane-icon"
            d="M17.7448 2.81298C18.7095 1.8165 20.3036 1.80361 21.2843 2.78436C22.2382 3.73823 22.2559 5.27921 21.3243 6.25481L18.5456 9.16457C18.3278 9.39265 18.219 9.50668 18.1518 9.64024C18.0924 9.75847 18.0571 9.88732 18.0478 10.0193C18.0374 10.1684 18.0728 10.3221 18.1438 10.6293L19.8717 18.1169C19.9444 18.4323 19.9808 18.59 19.9691 18.7426C19.9587 18.8776 19.921 19.0091 19.8582 19.1291C19.7873 19.2647 19.6729 19.3792 19.444 19.608L19.0732 19.9788C18.4671 20.585 18.164 20.888 17.8538 20.9429C17.583 20.9908 17.3043 20.925 17.0835 20.761C16.8306 20.5733 16.695 20.1666 16.424 19.3534L14.4142 13.3241L11.0689 16.6695C10.8692 16.8691 10.7694 16.969 10.7026 17.0866C10.6434 17.1907 10.6034 17.3047 10.5846 17.423C10.5633 17.5565 10.5789 17.6968 10.61 17.9775L10.7937 19.6309C10.8249 19.9116 10.8405 20.0519 10.8192 20.1854C10.8004 20.3037 10.7604 20.4177 10.7012 20.5219C10.6344 20.6394 10.5346 20.7393 10.3349 20.939L10.1374 21.1365C9.66434 21.6095 9.42781 21.8461 9.16496 21.9146C8.93442 21.9746 8.68999 21.9504 8.47571 21.8463C8.2314 21.7276 8.04585 21.4493 7.67475 20.8926L6.10643 18.5401C6.04013 18.4407 6.00698 18.391 5.96849 18.3459C5.9343 18.3058 5.89701 18.2685 5.85694 18.2343C5.81184 18.1958 5.76212 18.1627 5.66267 18.0964L3.31018 16.5281C2.75354 16.157 2.47521 15.9714 2.35649 15.7271C2.25236 15.5128 2.22816 15.2684 2.28824 15.0378C2.35674 14.775 2.59327 14.5385 3.06633 14.0654L3.26384 13.8679C3.46352 13.6682 3.56337 13.5684 3.68095 13.5016C3.78511 13.4424 3.89906 13.4024 4.01736 13.3836C4.15089 13.3623 4.29123 13.3779 4.5719 13.4091L6.22529 13.5928C6.50596 13.6239 6.6463 13.6395 6.77983 13.6182C6.89813 13.5994 7.01208 13.5594 7.11624 13.5002C7.23382 13.4334 7.33366 13.3336 7.53335 13.1339L10.8787 9.7886L4.84939 7.77884C4.03616 7.50776 3.62955 7.37222 3.44176 7.11932C3.27777 6.89848 3.212 6.61984 3.2599 6.34898C3.31477 6.03879 3.61784 5.73572 4.22399 5.12957L4.59476 4.7588C4.82365 4.52991 4.9381 4.41546 5.07369 4.34457C5.1937 4.28183 5.3252 4.24411 5.46023 4.23371C5.61278 4.22197 5.77049 4.25836 6.0859 4.33115L13.545 6.05249C13.855 6.12401 14.01 6.15978 14.1596 6.14914C14.3041 6.13886 14.4446 6.09733 14.5714 6.02742C14.7028 5.95501 14.8134 5.84074 15.0347 5.6122L17.7448 2.81298Z"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </g>
        <animateMotion dur="5.8s" repeatCount="indefinite" rotate="auto" path={motionPath} />
      </g>
    </svg>
  )
}

function StepIcon({ state }: { state: StepState }) {
  if (state === 'done') {
    return (
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <circle cx="10" cy="10" r="10" fill="#ff8a1d" />
        <path d="M5.8 10.4l2.7 2.7 5.7-5.7" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }

  if (state === 'active') {
    return (
      <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <circle cx="10" cy="10" r="9" stroke="#ff8a1d" strokeWidth="2" />
        <circle cx="10" cy="10" r="3.5" fill="#ff8a1d" />
      </svg>
    )
  }

  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <circle cx="10" cy="10" r="8.5" stroke="rgba(20, 45, 60, 0.18)" strokeWidth="2" />
    </svg>
  )
}

export default function SearchingTasks({
  originLabel,
  originCode,
  destinationLabel,
  destinationCode,
  progress,
  searchedAt,
}: Props) {
  const TOTAL = progress?.total || 198
  const t = useTranslations('SearchingTasks')
  const originName = originLabel || originCode || 'Origin'
  const destinationName = destinationLabel || destinationCode || 'Destination'

  const [elapsed, setElapsed] = useState(() =>
    searchedAt ? Math.max(0, (Date.now() - new Date(searchedAt).getTime()) / 1000) : 0
  )
  const [airlineIdx, setAirlineIdx] = useState(0)

  // Advance elapsed time at 100ms ticks, anchored to absolute search start time
  useEffect(() => {
    const epochMs = searchedAt ? new Date(searchedAt).getTime() : Date.now()
    const id = setInterval(() => {
      setElapsed((Date.now() - epochMs) / 1000)
    }, 100)
    return () => clearInterval(id)
  }, [searchedAt])

  // Simulated counter: real progress when available, otherwise easeOutCubic over 130s
  const simChecked = useMemo(() => {
    const real = progress?.checked
    const t = Math.min(elapsed / 130, 1)
    const eased = 1 - Math.pow(1 - t, 3)
    const simVal = Math.round(eased * TOTAL)
    // Use whichever is larger: real FSW count or simulated, never exceed TOTAL
    if (real !== undefined && real > 0) return Math.min(Math.max(real, simVal), TOTAL)
    return simVal
  }, [elapsed, progress, TOTAL])

  // Phase driven by elapsed time, spread across typical 90–150s search
  // 0→Searching (0–30s) · 1→Comparing (30–75s) · 2→Sorting (75–120s) · 3→Almost there (120s+)
  const phaseIndex = useMemo(() => {
    if (elapsed >= 120) return 3
    if (elapsed >= 75) return 2
    if (elapsed >= 30) return 1
    return 0
  }, [elapsed])

  // Fast airline cycling during phases 0 and 1
  useEffect(() => {
    if (phaseIndex > 1) return
    const id = setInterval(() => {
      setAirlineIdx(i => (i + 1) % SAMPLE_AIRLINES.length)
    }, 200)
    return () => clearInterval(id)
  }, [phaseIndex])

  const currentAirline = SAMPLE_AIRLINES[airlineIdx]

  const steps = useMemo(() => {
    const resolveState = (index: number): StepState => {
      if (index < phaseIndex) return 'done'
      if (index === phaseIndex) return 'active'
      return 'pending'
    }

    return [
      {
        title: t('step_searching'),
        hint: t('step_searchingHint', { airline: currentAirline }),
        state: resolveState(0),
      },
      {
        title: t('step_comparing'),
        hint: t('step_comparingHint'),
        state: resolveState(1),
      },
      {
        title: t('step_sorting'),
        hint: t('step_sortingHint'),
        state: resolveState(2),
      },
      {
        title: t('step_almost'),
        hint: t('step_almostHint'),
        state: resolveState(3),
      },
    ]
  }, [currentAirline, phaseIndex, t])

  return (
    <div className="st-card">
      <div className="st-body">
        <div className="st-header-row">
          <span className="st-pill">{t('pill')}</span>
        </div>

        <div className="st-copy">
          <h2 className="st-title">
            {t('titlePre')}<span className="st-title-accent">{t('titleAccent')}</span>{t('titlePost')}
          </h2>
          <p className="st-subtitle">
            {t('subtitle')}
          </p>
        </div>

        <div className="st-scene" aria-label={t('sceneLabel', { origin: originName, destination: destinationName })}>
          <div className="st-city st-city--origin">
            <div className="st-city-meta">
              <span className="st-city-name">{originName}</span>
              {originCode ? <span className="st-city-code">{originCode}</span> : null}
            </div>
            <Skyline seed={hashStr(originCode ?? originName)} cityCode={originCode ?? ''} />
          </div>

          <div className="st-flight-path">
            <FlightArc />
          </div>

          <div className="st-city st-city--destination">
            <div className="st-city-meta st-city-meta--right">
              <span className="st-city-name">{destinationName}</span>
              {destinationCode ? <span className="st-city-code">{destinationCode}</span> : null}
            </div>
            <Skyline mirrored seed={hashStr(destinationCode ?? destinationName)} cityCode={destinationCode ?? ''} />
          </div>

        </div>

        <div className="st-steps" role="list" aria-label="Search progress">
          {steps.map((step) => (
            <div key={step.title} className={`st-step st-step--${step.state}`} role="listitem">
              <span className="st-step-icon" aria-hidden="true">
                <StepIcon state={step.state} />
              </span>
              <div className="st-step-copy">
                <span className="st-step-title">{step.title}</span>
                <span className="st-step-hint">{step.hint}</span>
              </div>
            </div>
          ))}
        </div>

        <div className="st-footer">
          <span className="st-footer-checked">
            <svg viewBox="0 0 24 24" fill="none" aria-hidden="true" className="st-footer-plane">

              <path
                d="M17.7448 2.81298C18.7095 1.8165 20.3036 1.80361 21.2843 2.78436C22.2382 3.73823 22.2559 5.27921 21.3243 6.25481L18.5456 9.16457C18.3278 9.39265 18.219 9.50668 18.1518 9.64024C18.0924 9.75847 18.0571 9.88732 18.0478 10.0193C18.0374 10.1684 18.0728 10.3221 18.1438 10.6293L19.8717 18.1169C19.9444 18.4323 19.9808 18.59 19.9691 18.7426C19.9587 18.8776 19.921 19.0091 19.8582 19.1291C19.7873 19.2647 19.6729 19.3792 19.444 19.608L19.0732 19.9788C18.4671 20.585 18.164 20.888 17.8538 20.9429C17.583 20.9908 17.3043 20.925 17.0835 20.761C16.8306 20.5733 16.695 20.1666 16.424 19.3534L14.4142 13.3241L11.0689 16.6695C10.8692 16.8691 10.7694 16.969 10.7026 17.0866C10.6434 17.1907 10.6034 17.3047 10.5846 17.423C10.5633 17.5565 10.5789 17.6968 10.61 17.9775L10.7937 19.6309C10.8249 19.9116 10.8405 20.0519 10.8192 20.1854C10.8004 20.3037 10.7604 20.4177 10.7012 20.5219C10.6344 20.6394 10.5346 20.7393 10.3349 20.939L10.1374 21.1365C9.66434 21.6095 9.42781 21.8461 9.16496 21.9146C8.93442 21.9746 8.68999 21.9504 8.47571 21.8463C8.2314 21.7276 8.04585 21.4493 7.67475 20.8926L6.10643 18.5401C6.04013 18.4407 6.00698 18.391 5.96849 18.3459C5.9343 18.3058 5.89701 18.2685 5.85694 18.2343C5.81184 18.1958 5.76212 18.1627 5.66267 18.0964L3.31018 16.5281C2.75354 16.157 2.47521 15.9714 2.35649 15.7271C2.25236 15.5128 2.22816 15.2684 2.28824 15.0378C2.35674 14.775 2.59327 14.5385 3.06633 14.0654L3.26384 13.8679C3.46352 13.6682 3.56337 13.5684 3.68095 13.5016C3.78511 13.4424 3.89906 13.4024 4.01736 13.3836C4.15089 13.3623 4.29123 13.3779 4.5719 13.4091L6.22529 13.5928C6.50596 13.6239 6.6463 13.6395 6.77983 13.6182C6.89813 13.5994 7.01208 13.5594 7.11624 13.5002C7.23382 13.4334 7.33366 13.3336 7.53335 13.1339L10.8787 9.7886L4.84939 7.77884C4.03616 7.50776 3.62955 7.37222 3.44176 7.11932C3.27777 6.89848 3.212 6.61984 3.2599 6.34898C3.31477 6.03879 3.61784 5.73572 4.22399 5.12957L4.59476 4.7588C4.82365 4.52991 4.9381 4.41546 5.07369 4.34457C5.1937 4.28183 5.3252 4.24411 5.46023 4.23371C5.61278 4.22197 5.77049 4.25836 6.0859 4.33115L13.545 6.05249C13.855 6.12401 14.01 6.15978 14.1596 6.14914C14.3041 6.13886 14.4446 6.09733 14.5714 6.02742C14.7028 5.95501 14.8134 5.84074 15.0347 5.6122L17.7448 2.81298Z"
                fill="#ff8a1d"
              />
            </svg>
            {t('footerChecked', { checked: simChecked, total: TOTAL })}
          </span>
          <span className="st-footer-eta">{t('footerEta')}</span>
        </div>
      </div>
    </div>
  )
}
