// usePlaces — owns the Places browser state (section / search / category / sort) and drives the
// fetches, the way decentraland.org/places does: the grid, the Live Now feed and the Featured set
// are three reads of the places API's /destinations feed. A `cancelled` flag drops stale responses
// (we don't abort: placesApi shares cached promises across consumers).

import { useCallback, useEffect, useRef, useState } from 'react'
import { getLogin } from '../auth/sso'
import {
  BROWSE_ARGS,
  FEATURED_ARGS,
  LIVE_FEED_ARGS,
  fetchDestinations,
  fetchPlaces,
  fetchWorlds,
  type DiscoverPlace,
  type PlacesOrderBy,
  type PlacesResponse
} from './placesApi'

export type PlacesSection = 'all' | 'favourites' | 'my'
export type PlacesSort = 'most_active' | 'like_score_best' | 'created_at' | 'name'

export interface UsePlaces {
  places: DiscoverPlace[]
  /** The busiest destinations (places and worlds) with live presence, for Live Now. */
  liveFeed: DiscoverPlace[]
  /** The places API's curated set, for the Featured rail. */
  featured: DiscoverPlace[]
  loading: boolean
  loadingMore: boolean
  hasMore: boolean
  loadMore: () => void
  retry: () => void
  error: string | null
  search: string
  setSearch: (s: string) => void
  category: string
  setCategory: (c: string) => void
  sort: PlacesSort
  setSort: (s: PlacesSort) => void
  section: PlacesSection
  setSection: (s: PlacesSection) => void
}

const dataOrEmpty = (pr: Promise<PlacesResponse>): Promise<DiscoverPlace[]> => pr.then((r) => r.data).catch(() => [])

export function usePlaces(account?: string | null): UsePlaces {
  const [places, setPlaces] = useState<DiscoverPlace[]>([])
  const [liveFeed, setLiveFeed] = useState<DiscoverPlace[]>([])
  const [featured, setFeatured] = useState<DiscoverPlace[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [search, updateSearch] = useState('')
  // 'all' = no category filter; 'featured' = the curated set (only_highlighted).
  const [category, updateCategory] = useState('all')
  const [sort, updateSort] = useState<PlacesSort>('most_active')
  const [section, updateSection] = useState<PlacesSection>('all')
  const effectiveSort = section === 'all' ? sort : 'most_active'
  const key = JSON.stringify([search.trim(), category, effectiveSort, section, account?.toLowerCase()])
  const [page, setPage] = useState({ key, offset: 0 })
  const offset = page.key === key ? page.offset : 0
  const [resultKey, setResultKey] = useState(key)
  const rowsKey = useRef(key)
  const [hasMore, setHasMore] = useState(false)
  const [attempt, setAttempt] = useState(0)
  const setSearch = useCallback((value: string) => updateSearch(value.trim()), [])
  const setCategory = useCallback((value: string) => updateCategory(value), [])
  const setSort = useCallback((value: PlacesSort) => updateSort(value), [])
  const setSection = useCallback((value: PlacesSection) => updateSection(value), [])
  useEffect(() => {
    setPage((previous) => previous.key === key ? previous : { key, offset: 0 })
  }, [key])

  useEffect(() => {
    let cancelled = false
    void dataOrEmpty(fetchDestinations(LIVE_FEED_ARGS)).then((d) => !cancelled && setLiveFeed(d))
    void dataOrEmpty(fetchDestinations(FEATURED_ARGS)).then((d) => !cancelled && setFeatured(d))
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    const q = search.trim()
    const categories = category === 'all' || category === 'featured' ? undefined : [category]

    // /destinations only orders by activity; the other sorts read the older per-type endpoints.
    const legacySort = async (): Promise<{ data: DiscoverPlace[]; more: boolean }> => {
      const args = { limit: BROWSE_ARGS.limit, offset, order_by: effectiveSort as PlacesOrderBy, order: 'desc' as const, search: q || undefined }
      if (q) {
        const [p, w] = await Promise.all([fetchPlaces(args), fetchWorlds(args)])
        return { data: [...p.data, ...w.data], more: offset + p.data.length < p.total || offset + w.data.length < w.total }
      }
      const response = await fetchPlaces({ ...args, only_pois: category === 'featured' ? true : undefined, categories })
      return { data: response.data, more: offset + response.data.length < response.total }
    }

    const destinations = async (): Promise<{ data: DiscoverPlace[]; more: boolean }> => {
      const login = section === 'all' ? null : await getLogin()
      if (section !== 'all' && (!login || login.guest || (account != null && login.address.toLowerCase() !== account.toLowerCase()))) {
        throw new Error(`Sign in to see ${section === 'my' ? 'your places' : 'your favourites'}.`)
      }
      const response = await fetchDestinations({
        ...BROWSE_ARGS,
        offset,
        search: q || undefined,
        categories,
        only_highlighted: category === 'featured' || undefined,
        only_favorites: section === 'favourites' || undefined,
        owner: section === 'my' ? login?.address : undefined
      }, login ?? undefined)
      return { data: response.data, more: offset + response.data.length < response.total }
    }

    const run = effectiveSort !== 'most_active' ? legacySort() : destinations()

    run
      .then(({ data, more }) => {
        if (cancelled) return
        const append = offset > 0 && rowsKey.current === key
        rowsKey.current = key
        setPlaces((previous) => append ? [...new Map([...previous, ...data].map((place) => [place.id, place])).values()] : data)
        setResultKey(key)
        setHasMore(data.length > 0 && more)
        setLoading(false)
      })
      .catch((e: unknown) => {
        if (cancelled) return
        setError(e instanceof Error ? e.message : 'Failed to load places')
        if (offset === 0) {
          setPlaces([])
          setResultKey(key)
          setHasMore(false)
        }
        setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [search, category, effectiveSort, section, key, offset, attempt, account])

  const current = resultKey === key
  const loadingMore = current && loading && offset > 0
  const loadMore = (): void => {
    if (!loading && hasMore) setPage({ key, offset: offset + BROWSE_ARGS.limit })
  }
  return { places: current ? places : [], liveFeed, featured, loading: loading && !loadingMore, loadingMore, hasMore: current && hasMore, loadMore, retry: () => setAttempt((previous) => previous + 1), error, search, setSearch, category, setCategory, sort, setSort, section, setSection }
}
