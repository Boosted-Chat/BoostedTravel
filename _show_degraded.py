import json
with open('_live_dashboard.json', 'r') as f:
    data = json.load(f)

degraded = data.get('degraded', [])
connectors = data.get('connectors', {})

print(f'Total degraded: {len(degraded)}')
print('='*90)
print(f'{"Connector":<35} {"OK":>5} {"Fail":>6} {"Total":>6} {"Rate":>8} {"Offers":>7}')
print('='*90)

for name in sorted(degraded):
    if name in connectors:
        c = connectors[name]
        local = c.get('local', {})
        ok = local.get('ok', 0)
        fail = local.get('fail', 0)
        total = local.get('total', 0)
        rate = local.get('fail_rate', 0) * 100
        offers = local.get('total_offers', 0)
        print(f'{name:<35} {ok:>5} {fail:>6} {total:>6} {rate:>7.1f}% {offers:>7}')
