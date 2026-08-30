import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

function observe(page) {
  const external = []; const errors = [];
  page.on('request', (request) => { if (!request.url().startsWith('http://127.0.0.1:4175')) external.push(request.url()); });
  page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
  page.on('pageerror', (error) => errors.push(error.message));
  return { external, errors };
}

test('dashboard filters recompute every visible analytical surface', async ({ page }) => {
  const observed = observe(page);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: /101,786 source records/i })).toBeVisible();
  await expect(page.locator('#runBadge')).toContainText('101,786 rows');
  await expect(page.locator('#errorState')).toBeHidden();
  const initialRevenue = await page.locator('#kpiRevenue').textContent();
  await page.locator('#warehouse').selectOption('2');
  await expect(page.locator('#kpiRevenue')).not.toHaveText(initialRevenue);
  await expect(page.locator('#salesChart svg')).toBeVisible();
  await expect(page.locator('#supplierRows tr')).not.toHaveCount(0);
  await expect(page.locator('#inventoryBars .health-row')).toHaveCount(4);
  expect(observed.external).toEqual([]);
  expect(observed.errors).toEqual([]);
});

test('technical report and downloadable evidence are linked', async ({ page, request }) => {
  const observed = observe(page);
  await page.goto('/report/');
  await expect(page.getByRole('heading', { name: /Data contracts/i })).toBeVisible();
  await expect(page.locator('#fail')).toHaveText('0');
  await expect(page.locator('#rules tr')).toHaveCount(40);
  const archive = await request.get('/downloads/supply-chain-dataset-seed-42.zip');
  expect(archive.ok()).toBeTruthy();
  expect((await archive.body()).length).toBeGreaterThan(1_000_000);
  expect(observed.external).toEqual([]);
  expect(observed.errors).toEqual([]);
});

test.describe('mobile and reduced motion', () => {
  test.use({ viewport: { width: 390, height: 844 }, reducedMotion: 'reduce' });
  test('dashboard remains navigable by keyboard and exposes table fallbacks', async ({ page }) => {
    const observed = observe(page);
    await page.goto('/');
    await page.keyboard.press('Tab');
    await expect(page.getByRole('link', { name: 'Skip to dashboard' })).toBeFocused();
    await page.getByRole('link', { name: 'Skip to dashboard' }).press('Enter');
    await expect(page.locator('#dashboard')).toBeInViewport();
    await page.getByText('Accessible monthly values').click();
    await expect(page.locator('#monthlyRows tr')).not.toHaveCount(0);
    expect(observed.external).toEqual([]);
    expect(observed.errors).toEqual([]);
  });
});

for (const route of ['/', '/report/']) {
  test(`WCAG AA audit passes for ${route}`, async ({ page }) => {
    await page.goto(route);
    await page.locator(route === '/' ? '#kpiRevenue' : '#rules tr').first().waitFor();
    const { violations } = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
    const summary = violations.map(({ id, impact, nodes }) => ({
      id,
      impact,
      targets: nodes.map((node) => node.target.join(' ')),
    }));
    expect(summary).toEqual([]);
  });
}
