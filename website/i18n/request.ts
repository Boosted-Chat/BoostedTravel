import { getRequestConfig } from 'next-intl/server'
import { hasLocale } from 'next-intl'
import { routing } from './routing'
import { cookies } from 'next/headers'

export default getRequestConfig(async ({ requestLocale }) => {
  const requested = await requestLocale
  let locale = hasLocale(routing.locales, requested) ? requested : null

  if (!locale) {
    // For pages outside [locale] routing (results, book), fall back to cookie
    const cookieStore = await cookies()
    const cookieLocale = cookieStore.get('LETSFG_LOCALE')?.value
    locale = (cookieLocale && hasLocale(routing.locales, cookieLocale)) ? cookieLocale : routing.defaultLocale
  }

  return {
    locale,
    messages: (await import(`../messages/${locale}.json`)).default,
  }
})
