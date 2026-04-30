const FLIGHT_DATETIME_PREFIX_RE = /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2}))?/

interface FlightDateTimeParts {
  year: number
  month: number
  day: number
  hour?: number
  minute?: number
}

function parseFlightDateTimeParts(value: string): FlightDateTimeParts | null {
  const match = value.match(FLIGHT_DATETIME_PREFIX_RE)
  if (!match) {
    return null
  }

  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const hour = match[4] === undefined ? undefined : Number(match[4])
  const minute = match[5] === undefined ? undefined : Number(match[5])

  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return null
  }
  if (hour !== undefined && (!Number.isInteger(hour) || hour < 0 || hour > 23)) {
    return null
  }
  if (minute !== undefined && (!Number.isInteger(minute) || minute < 0 || minute > 59)) {
    return null
  }

  const date = new Date(Date.UTC(year, month - 1, day))
  if (
    date.getUTCFullYear() !== year
    || date.getUTCMonth() !== month - 1
    || date.getUTCDate() !== day
  ) {
    return null
  }

  return { year, month, day, hour, minute }
}

export function formatFlightTime(value: string): string {
  const parts = parseFlightDateTimeParts(value)
  if (parts?.hour !== undefined && parts.minute !== undefined) {
    return `${String(parts.hour).padStart(2, '0')}:${String(parts.minute).padStart(2, '0')}`
  }

  try {
    const date = new Date(value)
    if (!Number.isNaN(date.getTime())) {
      return new Intl.DateTimeFormat('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
        timeZone: 'UTC',
      }).format(date)
    }
  } catch {
    // Fall through to returning the original value.
  }

  return value
}

export function formatFlightDateCompact(value: string): string {
  const parts = parseFlightDateTimeParts(value)
  if (parts) {
    return new Intl.DateTimeFormat('en-GB', {
      day: 'numeric',
      month: 'long',
      timeZone: 'UTC',
    }).format(new Date(Date.UTC(parts.year, parts.month - 1, parts.day)))
  }

  try {
    const date = new Date(value)
    if (!Number.isNaN(date.getTime())) {
      return new Intl.DateTimeFormat('en-GB', {
        day: 'numeric',
        month: 'long',
        timeZone: 'UTC',
      }).format(date)
    }
  } catch {
    // Fall through.
  }

  return ''
}

export function extractFlightClockMinutes(value: string): number {
  const parts = parseFlightDateTimeParts(value)
  if (parts?.hour !== undefined && parts.minute !== undefined) {
    return parts.hour * 60 + parts.minute
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return 0
  }

  return date.getUTCHours() * 60 + date.getUTCMinutes()
}