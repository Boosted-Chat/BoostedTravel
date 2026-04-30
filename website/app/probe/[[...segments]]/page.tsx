import { redirect } from 'next/navigation'

const KNOWN_LOCALES = new Set(['de', 'en', 'es', 'fr', 'hr', 'it', 'nl', 'pl', 'pt', 'sq', 'sv'])

function normalizeSegments(segments: string[] | undefined): string[] {
  return Array.isArray(segments) ? segments.filter((segment) => segment.trim().length > 0) : []
}

function getProbeRedirectTarget(segments: string[]): string {
  const [first, second, third] = segments

  if (!first) {
    return '/en'
  }

  if (KNOWN_LOCALES.has(first)) {
    if (second === 'results') {
      return third ? `/results/${third}` : '/results'
    }
    if (second === 'book' && third) {
      return `/book/${third}`
    }
    return `/${first}`
  }

  if (first === 'results') {
    return second ? `/results/${second}` : '/results'
  }

  if (first === 'book' && second) {
    return `/book/${second}`
  }

  return '/en'
}

export default async function ProbeRedirectPage({
  params,
  searchParams,
}: {
  params: Promise<{ segments?: string[] }>
  searchParams: Promise<Record<string, string | string[] | undefined>>
}) {
  const { segments } = await params
  const resolvedSearchParams = await searchParams
  const targetPath = getProbeRedirectTarget(normalizeSegments(segments))
  const nextSearchParams = new URLSearchParams()

  for (const [key, value] of Object.entries(resolvedSearchParams)) {
    if (Array.isArray(value)) {
      for (const entry of value) {
        nextSearchParams.append(key, entry)
      }
      continue
    }

    if (typeof value === 'string') {
      nextSearchParams.set(key, value)
    }
  }

  nextSearchParams.set('probe', '1')
  redirect(`${targetPath}?${nextSearchParams.toString()}`)
}