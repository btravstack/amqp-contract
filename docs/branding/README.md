# amqp-contract branding

These assets belong to the btravstack family of project marks. The canonical
source is [the website brand generator](https://github.com/btravstack/btravstack.github.io/blob/main/scripts/generate-brand.mjs).

To refresh them, run the generator in that repository and copy the generated
`apps/website/public/logos/amqp-contract-{light,dark,mono,favicon}.svg`
files into `docs/public/` as `logo-light.svg`, `logo-dark.svg`, `logo-mono.svg`
and `favicon.svg`. Keep `logo.svg` identical to `logo-light.svg` as the fallback.
The matching social card is `docs/public/og-amqp-contract.png` (1200 × 630),
rendered from the [editable social-card template](https://github.com/btravstack/btravstack.github.io/blob/main/branding/social-card.html)
with `?project=amqp-contract`.

The README, documentation navigation and hero select a mark for the reader's
color scheme. A dedicated compact mark serves as the favicon. All assets are
committed locally so documentation builds and deployed pages remain independent
of the website repository. The docs configuration applies the current deployment
base to favicon and social-card URLs, including prerelease deployments.
