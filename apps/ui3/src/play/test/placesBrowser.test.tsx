import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest'
import { fireEvent, render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import * as auth from '../features/auth/sso'
import { PlacesBrowser } from '../features/places/PlacesBrowser'
import { clearPlacesCache, type DiscoverPlace } from '../features/places/placesApi'

const place = (id: string, title: string, extra: Partial<DiscoverPlace> = {}): DiscoverPlace => ({
  id,
  title,
  description: '',
  image: `https://img/${id}.png`,
  positions: ['1,1'],
  base_position: '1,1',
  owner: '0xabc',
  ...extra
})

const busy = place('busy', 'Busy Club', { user_count: 5 })
const empty = place('empty', 'Quiet Gallery', { user_count: 0 })
const curated = place('curated', 'Curated World', { world: true, world_name: 'curated.dcl.eth', highlighted: true })
const other = place('other', 'Other Scene')
const junk = place('junk', 'Empty', { image: 'https://api.decentraland.org/v2/map.png' })

function servePlaces(): void {
  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) => {
      const data = url.includes('only_highlighted=true') ? [curated] : url.includes('limit=40') ? [busy, empty] : [busy, empty, curated, other, junk]
      return Promise.resolve(new Response(JSON.stringify({ ok: true, total: data.length, data })))
    })
  )
}

afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })
beforeEach(clearPlacesCache)

describe('places browser', () => {
  it('keeps loaded cards visible while Retry requests the failed next page', async () => {
    let failed = false
    vi.stubGlobal('fetch', vi.fn(async (input: string) => {
      const url = new URL(input)
      if (url.searchParams.has('only_highlighted') || url.searchParams.get('limit') === '40') return new Response(JSON.stringify({ ok: true, total: 0, data: [] }))
      if (url.searchParams.get('offset') === '48' && !failed) { failed = true; throw new Error('page unavailable') }
      return new Response(JSON.stringify({ ok: true, total: 49, data: url.searchParams.get('offset') === '48' ? [other] : [busy] }))
    }))
    render(<PlacesBrowser onPick={vi.fn()} />)
    await userEvent.click(await screen.findByRole('button', { name: 'Load more places' }))
    expect(await screen.findByText("Couldn't load more places")).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Busy Club/ })).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('button', { name: /Other Scene/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Load more places' })).toBeNull()
  })

  it('hides unsupported sorting in personal sections and restores it for public discovery', async () => {
    servePlaces()
    vi.spyOn(auth, 'getLogin').mockResolvedValue(null)
    render(<PlacesBrowser onPick={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'Most active' })).toBeInTheDocument()
    await userEvent.click(screen.getByRole('tab', { name: /Favourites/ }))
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Most active' })).toBeNull())
    await userEvent.click(screen.getByRole('tab', { name: /Explore all/ }))
    expect(screen.getByRole('button', { name: 'Most active' })).toBeInTheDocument()
  })
  it('keeps Live Now to populated destinations, Featured to the curated set, and the grid to the rest', async () => {
    servePlaces()
    render(<PlacesBrowser onPick={vi.fn()} />)
    const live = (await screen.findByRole('heading', { name: /Live Now/ })).closest('section') as HTMLElement
    const featured = (await screen.findByRole('heading', { name: 'Featured Places' })).closest('section') as HTMLElement

    expect(within(live).getByRole('button', { name: /Busy Club/ })).toBeInTheDocument()
    expect(within(live).queryByRole('button', { name: /Quiet Gallery/ })).not.toBeInTheDocument()
    expect(within(featured).getByRole('button', { name: /Curated World/ })).toBeInTheDocument()

    expect(screen.getAllByRole('button', { name: /Busy Club/ })).toHaveLength(1)
    expect(screen.getByRole('button', { name: /Quiet Gallery/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Other Scene/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /\bEmpty\b/ })).not.toBeInTheDocument()
  })

  it('shows a search match even when its record looks like an unfinished deploy', async () => {
    const untitled = place('tortilla', 'My Scene', {
      world: true,
      world_name: 'tortilla.dcl.eth',
      image: 'https://peer/content/contents/bafkreidj26s7aenyxfthfdibnqonzqm5ptc4iamml744gmcyuokewkr76y',
      user_count: 0
    })
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        const data = url.includes('search=tortilla') ? [untitled] : []
        return Promise.resolve(new Response(JSON.stringify({ ok: true, total: data.length, data })))
      })
    )
    render(<PlacesBrowser onPick={vi.fn()} />)
    fireEvent.change(screen.getByPlaceholderText('Search places'), { target: { value: 'tortilla' } })
    expect(await screen.findByRole('button', { name: /My Scene/ }, { timeout: 2000 })).toBeInTheDocument()
  })
})
