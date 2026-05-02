'use client'

import { createPortal } from 'react-dom'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { Coins } from 'lucide-react'
import {
  CURRENCY_CHANGE_EVENT,
  DISPLAY_CURRENCIES,
  LETSFG_CURRENCY_COOKIE,
  normalizeCurrencyCode,
  readBrowserSearchCurrency,
  type CurrencyCode,
  type DisplayCurrencyCode,
} from '../lib/currency-preference'
import { getTrackedSourcePath } from '../lib/probe-mode'

function persistCurrencyPreference(code: string) {
  document.cookie = `${LETSFG_CURRENCY_COOKIE}=${encodeURIComponent(code)}; path=/; max-age=31536000; SameSite=Lax`
}

function resolveActiveSearchQuery(searchQuery: string): string {
  const urlQuery = new URLSearchParams(window.location.search).get('q')?.trim()
  if (urlQuery) {
    return urlQuery
  }

  const propQuery = searchQuery.trim()
  if (propQuery) {
    return propQuery
  }

  const formQuery = document.querySelector<HTMLInputElement>('input[name="q"]')?.value?.trim()
  return formQuery || ''
}

function buildCurrencyNavigationTarget(
  behavior: CurrencyButtonBehavior,
  code: DisplayCurrencyCode,
  searchQuery: string,
  probeMode: boolean,
): string | null {
  if (behavior === 'persist') {
    return null
  }

  const activeSearchQuery = resolveActiveSearchQuery(searchQuery)

  if (behavior === 'rerun-search' && activeSearchQuery) {
    const path = `/results?q=${encodeURIComponent(activeSearchQuery)}&cur=${encodeURIComponent(code)}`
    return getTrackedSourcePath(path, probeMode)
  }

  const { pathname, search } = window.location
  const params = new URLSearchParams(search)
  params.set('cur', code)
  return getTrackedSourcePath(`${pathname}?${params.toString()}`, probeMode)
}

export type CurrencyButtonBehavior = 'persist' | 'refresh' | 'rerun-search'

export interface CurrencyButtonProps {
  inline?: boolean
  behavior: CurrencyButtonBehavior
  initialCurrency?: CurrencyCode
  searchQuery?: string
  probeMode?: boolean
}

export default function CurrencyButton({
  inline = false,
  behavior,
  initialCurrency = 'EUR',
  searchQuery = '',
  probeMode = false,
}: CurrencyButtonProps) {
  const [current, setCurrent] = useState<CurrencyCode>(initialCurrency)
  const [open, setOpen] = useState(false)
  const [isNavigating, setIsNavigating] = useState(false)
  const [menuPos, setMenuPos] = useState<{ top: number; left: number } | null>(null)
  const wrapRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setIsNavigating(false)
  }, [initialCurrency])

  useEffect(() => {
    const urlCurrency = normalizeCurrencyCode(new URLSearchParams(window.location.search).get('cur'))
    if (urlCurrency) {
      persistCurrencyPreference(urlCurrency)
      setCurrent(urlCurrency)
      window.dispatchEvent(new CustomEvent(CURRENCY_CHANGE_EVENT))
    } else {
      setCurrent(readBrowserSearchCurrency(initialCurrency))
    }

    const onChange = () => setCurrent(readBrowserSearchCurrency(initialCurrency))
    window.addEventListener(CURRENCY_CHANGE_EVENT, onChange)
    return () => window.removeEventListener(CURRENCY_CHANGE_EVENT, onChange)
  }, [initialCurrency])

  useLayoutEffect(() => {
    if (!open) {
      setMenuPos(null)
      return
    }

    function updateMenuPosition() {
      const button = buttonRef.current
      const menu = menuRef.current
      if (!button || !menu) return

      const gap = 8
      const viewportPadding = 8
      const rect = button.getBoundingClientRect()
      const menuWidth = menu.offsetWidth || 168
      const menuHeight = menu.offsetHeight || 0

      let left = rect.right - menuWidth
      left = Math.min(left, window.innerWidth - menuWidth - viewportPadding)
      left = Math.max(viewportPadding, left)

      let top = rect.bottom + gap
      const aboveTop = rect.top - menuHeight - gap
      if (menuHeight > 0 && top + menuHeight > window.innerHeight - viewportPadding && aboveTop >= viewportPadding) {
        top = aboveTop
      }

      top = Math.max(viewportPadding, top)

      setMenuPos({ top, left })
    }

    let frameId: number | null = null
    const scheduleMenuPosition = () => {
      if (frameId !== null) return
      frameId = window.requestAnimationFrame(() => {
        frameId = null
        updateMenuPosition()
      })
    }

    updateMenuPosition()

    window.addEventListener('resize', scheduleMenuPosition)
    window.addEventListener('scroll', scheduleMenuPosition, true)
    window.visualViewport?.addEventListener('resize', scheduleMenuPosition)
    window.visualViewport?.addEventListener('scroll', scheduleMenuPosition)

    return () => {
      if (frameId !== null) {
        window.cancelAnimationFrame(frameId)
      }
      window.removeEventListener('resize', scheduleMenuPosition)
      window.removeEventListener('scroll', scheduleMenuPosition, true)
      window.visualViewport?.removeEventListener('resize', scheduleMenuPosition)
      window.visualViewport?.removeEventListener('scroll', scheduleMenuPosition)
    }
  }, [open])

  useEffect(() => {
    if (!open) return

    function onPointerDown(event: PointerEvent) {
      const target = event.target as Node
      if (wrapRef.current?.contains(target) || menuRef.current?.contains(target)) {
        return
      }
      setOpen(false)
    }

    document.addEventListener('pointerdown', onPointerDown)
    return () => document.removeEventListener('pointerdown', onPointerDown)
  }, [open])

  useEffect(() => {
    if (!open) return

    function onKey(event: KeyboardEvent) {
      if (event.key === 'Escape') setOpen(false)
    }

    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [open])

  const persistAndNavigate = (code: DisplayCurrencyCode) => {
    if (isNavigating) {
      return
    }

    if (code === current) {
      setOpen(false)
      return
    }

    persistCurrencyPreference(code)
    setCurrent(code)
    window.dispatchEvent(new CustomEvent(CURRENCY_CHANGE_EVENT))
    setOpen(false)

    const nextUrl = buildCurrencyNavigationTarget(behavior, code, searchQuery, probeMode)
    if (!nextUrl) {
      return
    }

    setIsNavigating(true)
    window.location.assign(nextUrl)
  }

  const dropdown = open
    ? createPortal(
        <div
          ref={menuRef}
          className="lp-lang-dropdown lp-lang-dropdown--portal"
          role="listbox"
          aria-label="Select currency"
          style={{
            top: menuPos?.top ?? 0,
            left: menuPos?.left ?? 0,
            visibility: menuPos ? 'visible' : 'hidden',
          }}
        >
          {DISPLAY_CURRENCIES.map((row) => (
            <button
              key={row.code}
              role="option"
              aria-selected={row.code === current}
              className={`lp-lang-option${row.code === current ? ' lp-lang-option--active' : ''}`}
              type="button"
              disabled={isNavigating}
              onClick={() => persistAndNavigate(row.code)}
            >
              <span className="lp-lang-flag" aria-hidden="true">{row.code}</span>
              <span className="lp-lang-name">{row.label}</span>
              {row.code === current && (
                <svg className="lp-lang-check" viewBox="0 0 16 16" fill="currentColor" width="13" height="13" aria-hidden="true">
                  <path d="M13.78 4.22a.75.75 0 0 1 0 1.06l-7.25 7.25a.75.75 0 0 1-1.06 0L2.22 9.28a.75.75 0 0 1 1.06-1.06L6 10.94l6.72-6.72a.75.75 0 0 1 1.06 0z"/>
                </svg>
              )}
            </button>
          ))}
        </div>,
        document.body,
      )
    : null

  return (
    <div ref={wrapRef} className={`lp-globe-wrap${inline ? ' lp-globe-wrap--inline' : ''}`}>
      <button
        ref={buttonRef}
        className={`lp-globe-btn lp-currency-btn${open ? ' lp-globe-btn--open' : ''}${isNavigating ? ' lp-currency-btn--busy' : ''}`}
        aria-label={isNavigating ? `Currency: ${current}. Updating prices` : `Currency: ${current}. Change currency`}
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-busy={isNavigating}
        type="button"
        title={`Currency: ${current}`}
        disabled={isNavigating}
        onClick={() => setOpen((value) => !value)}
      >
        <span className="lp-currency-btn-inner" aria-hidden="true">
          <Coins className="lp-currency-trigger-icon" size={15} strokeWidth={2} />
        </span>
      </button>

      {dropdown}
    </div>
  )
}