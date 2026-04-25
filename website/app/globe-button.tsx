'use client'

import { useEffect, useRef, useState } from 'react'
import { useRouter, useParams } from 'next/navigation'

const LANGUAGES = [
  { code: 'en', label: 'English',    flag: 'EN' },
  { code: 'pl', label: 'Polski',     flag: 'PL' },
  { code: 'de', label: 'Deutsch',    flag: 'DE' },
  { code: 'es', label: 'Español',    flag: 'ES' },
  { code: 'fr', label: 'Français',   flag: 'FR' },
  { code: 'it', label: 'Italiano',   flag: 'IT' },
  { code: 'pt', label: 'Português',  flag: 'PT' },
  { code: 'nl', label: 'Nederlands', flag: 'NL' },
  { code: 'sv', label: 'Svenska',    flag: 'SV' },
  { code: 'hr', label: 'Hrvatski',   flag: 'HR' },
  { code: 'sq', label: 'Shqip',      flag: 'SQ' },
]

function EarthIcon() {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" aria-hidden="true" width="18" height="18" fill="currentColor">
      <path d="M256.2 48c114.8 .1 207.8 93.2 207.8 208 0 22.1-3.4 43.4-9.8 63.4-2 .4-4.1 .6-6.2 .6l-2.7 0c-8.5 0-16.6-3.4-22.6-9.4l-29.3-29.3c-6-6-9.4-14.1-9.4-22.6l0-50.7c0-8.8 7.2-16 16-16s16-7.2 16-16-7.2-16-16-16l-24 0c-13.3 0-24 10.7-24 24s-10.7 24-24 24l-56 0c-8.8 0-16 7.2-16 16s-7.2 16-16 16l-25.4 0c-12.5 0-22.6-10.1-22.6-22.6 0-6 2.4-11.8 6.6-16l70.1-70.1c2.1-2.1 3.3-5 3.3-8 0-6.2-5.1-11.3-11.3-11.3l-14.1 0c-12.5 0-22.6-10.1-22.6-22.6 0-6 2.4-11.8 6.6-16l23.1-23.1c.8-.8 1.6-1.5 2.5-2.2zM438.4 356.1c-32.8 59.6-93.9 101.4-165.2 107.2-.7-2.3-1.1-4.8-1.1-7.3 0-13.3-10.7-24-24-24l-26.7 0c-8.5 0-16.6-3.4-22.6-9.4l-29.3-29.3c-6-6-9.4-14.1-9.4-22.6l0-66.7c0-17.7 14.3-32 32-32l98.7 0c8.5 0 16.6 3.4 22.6 9.4l29.3 29.3c6 6 14.1 9.4 22.6 9.4l5.5 0c8.5 0 16.6 3.4 22.6 9.4l16 16c4.2 4.2 10 6.6 16 6.6 4.8 0 9.3 1.5 13 4.1zM256 512l26.2-1.3c-8.6 .9-17.3 1.3-26.2 1.3zm26.2-1.3C411.3 497.6 512 388.6 512 256 512 114.6 397.4 0 256 0l0 0C114.6 0 0 114.6 0 256 0 383.5 93.2 489.3 215.3 508.8 228.5 510.9 242.1 512 256 512zM187.3 123.3l-32 32c-6.2 6.2-16.4 6.2-22.6 0s-6.2-16.4 0-22.6l32-32c6.2-6.2 16.4-6.2 22.6 0s6.2 16.4 0 22.6z"/>
    </svg>
  )
}

export default function GlobeButton({ inline = false }: { inline?: boolean } = {}) {
  const router = useRouter()
  const params = useParams()
  const currentLocale = (params?.locale as string) || 'en'

  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  // Close on outside click
  useEffect(() => {
    if (!open) return
    function onPointerDown(e: PointerEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('pointerdown', onPointerDown)
    return () => document.removeEventListener('pointerdown', onPointerDown)
  }, [open])

  // Close on Escape
  useEffect(() => {
    if (!open) return
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [open])

  const currentLang = LANGUAGES.find(l => l.code === currentLocale) ?? LANGUAGES[0]

  return (
    <div ref={ref} className={`lp-globe-wrap${inline ? ' lp-globe-wrap--inline' : ''}`}>
      <button
        className={`lp-globe-btn${open ? ' lp-globe-btn--open' : ''}`}
        aria-label="Language / region"
        aria-expanded={open}
        aria-haspopup="listbox"
        type="button"
        onClick={() => setOpen(v => !v)}
      >
        <EarthIcon />
      </button>

      {open && (
        <div className="lp-lang-dropdown" role="listbox" aria-label="Select language">
          {LANGUAGES.map(lang => (
            <button
              key={lang.code}
              role="option"
              aria-selected={lang.code === currentLocale}
              className={`lp-lang-option${lang.code === currentLocale ? ' lp-lang-option--active' : ''}`}
              onClick={() => { document.cookie = `LETSFG_LOCALE=${lang.code}; path=/; max-age=31536000; SameSite=Lax`; router.push(`/${lang.code}`); setOpen(false) }}
              type="button"
            >
              <span className="lp-lang-flag" aria-hidden="true">{lang.flag}</span>
              <span className="lp-lang-name">{lang.label}</span>
              {lang.code === currentLocale && (
                <svg className="lp-lang-check" viewBox="0 0 16 16" fill="currentColor" width="13" height="13" aria-hidden="true">
                  <path d="M13.78 4.22a.75.75 0 0 1 0 1.06l-7.25 7.25a.75.75 0 0 1-1.06 0L2.22 9.28a.75.75 0 0 1 1.06-1.06L6 10.94l6.72-6.72a.75.75 0 0 1 1.06 0z"/>
                </svg>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
