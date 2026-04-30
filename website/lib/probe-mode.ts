const TRUTHY_PROBE_VALUES = new Set(['1', 'true', 'yes', 'probe'])

export function firstQueryValue(value: string | string[] | null | undefined): string | undefined {
  if (Array.isArray(value)) {
    return value[0]
  }

  return value ?? undefined
}

export function isProbeModeValue(value: string | string[] | null | undefined): boolean {
  const resolved = firstQueryValue(value)
  if (!resolved) {
    return false
  }

  return TRUTHY_PROBE_VALUES.has(resolved.trim().toLowerCase())
}

export function getProbeAnalyticsSearchId(searchId: string): string {
  return `probe:${searchId}`
}

export function getTrackingSearchId(searchId: string | null | undefined, isProbe: boolean): string | null {
  if (!searchId) {
    return null
  }

  return isProbe ? getProbeAnalyticsSearchId(searchId) : searchId
}

export function getTrackedSourcePath(path: string, isProbe: boolean): string {
  if (!isProbe) {
    return path
  }

  return path.includes('?') ? `${path}&probe=1` : `${path}?probe=1`
}

export function appendProbeParam(params: URLSearchParams, isProbe: boolean) {
  if (isProbe) {
    params.set('probe', '1')
  }
}