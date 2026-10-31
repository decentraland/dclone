import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import * as auth from '../features/auth/sso'
import { clearPlacesCache, fetchDestinations, type DiscoverPlace } from '../features/places/placesApi'
import { usePlaces, type UsePlaces } from '../features/places/usePlaces'

beforeEach(clearPlacesCache)

describe('when browsing additional destinations', () => {
  let firstPage: DiscoverPlace[]
  let lastPlace: DiscoverPlace
  let filteredPlace: DiscoverPlace
  let hook: { result: { current: UsePlaces } }
  let resolveLatePage: (response: Response) => void

  beforeEach(() => {
    firstPage = Array.from({ length: 48 }, (_, index) => ({
      id: `page-one-${index}`, title: `Place ${index}`, description: '', image: 'cover.png', positions: ['0,0'], owner: null
    }))
    lastPlace = { ...firstPage[0]!, id: 'last-place', title: 'Last place' }
    filteredPlace = { ...lastPlace, id: 'filtered-place' }
    resolveLatePage = () => {}
    vi.stubGlobal('fetch', vi.fn((input: string) => {
      const url = new URL(input)
      const filtered = url.searchParams.has('search')
      const data = filtered ? [filteredPlace] : url.searchParams.get('offset') === '48' ? [lastPlace] : firstPage
      return Promise.resolve(new Response(JSON.stringify({ ok: true, total: filtered ? 1 : 49, data })))
    }))
    hook = renderHook(() => usePlaces())
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('should append the next page and retain the first page', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.places.map((place) => place.id)).toEqual([...firstPage.map((place) => place.id), lastPlace.id]))
  })

  it('should stop offering more pages after reaching the total', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.places).toHaveLength(49))
    expect(hook.result.current.hasMore).toBe(false)
  })

  it('should preserve loaded pages after equivalent filter updates', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.places).toHaveLength(49))
    act(() => {
      hook.result.current.setSearch('  ')
      hook.result.current.setCategory('all')
      hook.result.current.setSort('most_active')
      hook.result.current.setSection('all')
    })
    expect(hook.result.current.places).toHaveLength(49)
    expect(hook.result.current.loading).toBe(false)
  })

  it('should drop a late second page after a filter change', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    vi.mocked(fetch).mockImplementationOnce(() => new Promise<Response>((resolve) => { resolveLatePage = resolve }))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.loadingMore).toBe(true))
    act(() => hook.result.current.setSearch('different'))
    await waitFor(() => expect(hook.result.current.places).toEqual([filteredPlace]))
    await act(async () => resolveLatePage(new Response(JSON.stringify({ ok: true, total: 49, data: [lastPlace] }))))
    expect(hook.result.current.places).toEqual([filteredPlace])
  })

  it('should retain page one and retry the failed second page', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    vi.mocked(fetch).mockRejectedValueOnce(new Error('second page unavailable'))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.error).toBe('second page unavailable'))
    expect(hook.result.current.places).toEqual(firstPage)
    act(() => hook.result.current.retry())
    await waitFor(() => expect(hook.result.current.places).toHaveLength(49))
    expect(hook.result.current.error).toBeNull()
    expect(vi.mocked(fetch).mock.calls.filter(([url]) => String(url).includes('offset=48'))).toHaveLength(2)
  })

  it('should paginate both legacy endpoints for a sorted search', async () => {
    await waitFor(() => expect(hook.result.current.loading).toBe(false))
    vi.mocked(fetch).mockImplementation(async (input) => {
      const url = new URL(String(input))
      const world = url.pathname.endsWith('/worlds')
      const data = url.searchParams.get('offset') === '48' ? [{ ...lastPlace, id: world ? 'last-world' : 'last-place' }] : firstPage.map((place) => ({ ...place, id: `${world ? 'world' : 'place'}-${place.id}` }))
      return new Response(JSON.stringify({ ok: true, total: 49, data }))
    })
    act(() => { hook.result.current.setSearch('club'); hook.result.current.setSort('name') })
    await waitFor(() => expect(hook.result.current.places).toHaveLength(96))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.places).toHaveLength(98))
    expect(vi.mocked(fetch).mock.calls.filter(([url]) => String(url).includes('offset=48') && String(url).includes('order_by=name'))).toHaveLength(2)
    expect(hook.result.current.hasMore).toBe(false)
  })

  it('should restart the first page without mixing rows after switching a filter away and back', async () => {
    await waitFor(() => expect(hook.result.current.hasMore).toBe(true))
    act(() => hook.result.current.loadMore())
    await waitFor(() => expect(hook.result.current.places).toHaveLength(49))
    act(() => hook.result.current.setSearch('different'))
    await waitFor(() => expect(hook.result.current.places).toEqual([filteredPlace]))
    act(() => hook.result.current.setSearch(''))
    await waitFor(() => expect(hook.result.current.places).toEqual(firstPage))
  })
})

describe('when reading personal favourites', () => {
  let login: auth.StoredLogin
  let request: ReturnType<typeof vi.fn>

  beforeEach(() => {
    login = {
      address: '0x1111111111111111111111111111111111111111',
      identity: {
        ephemeralIdentity: { address: '0x2222222222222222222222222222222222222222', publicKey: '', privateKey: '01'.repeat(32) },
        expiration: '2099-01-01T00:00:00Z',
        authChain: [{ type: 'SIGNER', payload: '0x1111111111111111111111111111111111111111', signature: '' }]
      }
    }
    request = vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify({ ok: true, total: 0, data: [] }))))
    vi.stubGlobal('fetch', request)
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('should authenticate the favourites request with the delegated identity', async () => {
    await fetchDestinations({ only_favorites: true }, login)
    expect(request).toHaveBeenCalledWith(expect.stringContaining('only_favorites=true'), {
      headers: expect.objectContaining({ 'x-identity-auth-chain-1': expect.stringContaining('ECDSA_SIGNED_ENTITY') })
    })
    const [url, init] = request.mock.calls[0] as [string, RequestInit]
    const headers = new Headers(init.headers)
    const link = JSON.parse(headers.get('x-identity-auth-chain-1')!) as { payload: string }
    expect(link.payload).toBe(`get:${new URL(url).pathname}:${headers.get('x-identity-timestamp')}:${JSON.stringify({})}`.toLowerCase())
  })

  it('should fetch favourites again instead of caching personal results', async () => {
    await fetchDestinations({ only_favorites: true }, login)
    await fetchDestinations({ only_favorites: true }, login)
    expect(request).toHaveBeenCalledTimes(2)
  })

  it('should reuse a public request snapshot without leaking it into personal favourites', async () => {
    await fetchDestinations({ limit: 7 })
    await fetchDestinations({ limit: 7 })
    expect(request).toHaveBeenCalledTimes(1)
    await fetchDestinations({ limit: 7, only_favorites: true }, login)
    expect(request).toHaveBeenCalledTimes(2)
  })

  it('should evict a failed public request so a retry reaches the service', async () => {
    request.mockRejectedValueOnce(new Error('unavailable'))
    await expect(fetchDestinations({ limit: 7 })).rejects.toThrow('unavailable')
    await fetchDestinations({ limit: 7 })
    expect(request).toHaveBeenCalledTimes(2)
  })

  it('should reject a signed-out favourites request', async () => {
    await expect(fetchDestinations({ only_favorites: true })).rejects.toThrow('Sign in to see your favourites.')
  })

  it('should use a fresh signed-in identity while the engine profile is not yet available', async () => {
    vi.spyOn(auth, 'getLogin').mockResolvedValue(login)
    const view = renderHook(() => usePlaces(null))
    act(() => view.result.current.setSection('favourites'))
    await waitFor(() => expect(request.mock.calls.some(([url]) => String(url).includes('only_favorites=true'))).toBe(true))
    await waitFor(() => expect(view.result.current.loading).toBe(false))
    expect(view.result.current.error).toBeNull()
    view.unmount()
  })

  describe('and the active account changes', () => {
    let view: { result: { current: UsePlaces }; rerender: (props: { account: string }) => void }
    let nextLogin: auth.StoredLogin

    beforeEach(() => {
      nextLogin = { ...login, address: '0x3333333333333333333333333333333333333333', identity: { ...login.identity, authChain: [{ type: 'SIGNER', payload: '0x3333333333333333333333333333333333333333', signature: '' }] } }
      vi.spyOn(auth, 'getLogin').mockResolvedValue(login)
      request.mockImplementation((input: string, init?: RequestInit) => {
        const headers = new Headers(init?.headers)
        const personal = input.includes('only_favorites=true')
        const owner = headers.get('x-identity-auth-chain-0')?.includes(nextLogin.address) ? nextLogin.address : login.address
        const data = personal ? [{ id: owner, title: 'Saved place', description: '', image: '', positions: [], owner }] : []
        return Promise.resolve(new Response(JSON.stringify({ ok: true, total: data.length, data })))
      })
      view = renderHook(({ account }) => usePlaces(account), { initialProps: { account: login.address } })
    })

    afterEach(() => cleanup())

    it('should clear the previous account favourites before loading the new account', async () => {
      act(() => view.result.current.setSection('favourites'))
      await waitFor(() => expect(view.result.current.places[0]?.id).toBe(login.address))
      vi.mocked(auth.getLogin).mockResolvedValue(nextLogin)
      view.rerender({ account: nextLogin.address })
      expect(view.result.current.places).toEqual([])
      await waitFor(() => expect(view.result.current.places[0]?.id).toBe(nextLogin.address))
    })
  })
})
