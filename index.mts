/**
 * USE RESPONSIBLY
 */

import { existsSync, readFileSync } from "node:fs";
import { appendFile } from "node:fs/promises";
import { resolve as resolveUrl } from "node:url";

import { createId } from '@paralleldrive/cuid2';
import { load as cheerio } from "cheerio";

import { Smoltable, createColumnKey } from "./table.mjs";

//
// CONFIGURATION ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//

const SMOLTABLE_URL = "http://localhost:9876";
const MAIN_TABLE = "webtable";
const QUEUE_TABLE = `${MAIN_TABLE}_queue`;

const STORE_SUB_PAGES = true;
const ENTRY_POINT = "https://en.wikipedia.org/wiki/Main_Page";
const STAY_ON_PAGE = "https://en.wikipedia.org";

const STORE_DOCUMENTS = false;

const PARALLELISM = 4;
const LIMIT = 10_000;

//
// ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//

async function storeDocument(
  formattedUrl: string,
  html: string
): Promise<void> {
  console.log(`Storing HTML document of ${formattedUrl}`);

  await mainTable.write([
    {
      row_key: formattedUrl,
      cells: [
        {
          column_key: createColumnKey("contents"),
          type: "string",
          value: html,
        },
      ],
    },
  ]);
}

async function siteAlreadyScraped(formattedUrl: string): Promise<boolean> {
  console.error(`Checking if site is already scraped: ${formattedUrl}`);

  const rowsUrl = `${SMOLTABLE_URL}/v1/table/${MAIN_TABLE}/rows`;

  const response = await fetch(rowsUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      items: [
        {
          row: {
            key: formattedUrl,
          },
          column: {
            key: createColumnKey("title"),
          },
        },
      ],
    }),
  });

  if (!response.ok) {
    throw new Error(
      `SMOLTABLE FAILED D: : ${response.status} ${await response.text()}`
    );
  }

  const data = (await response.json()) as {
    result: {
      rows: unknown[];
    };
  };

  return data.result.rows.length > 0;
}

async function removeSiteFromQueue(rowKey: string): Promise<void> {
  console.error(`Removing item from queue: ${rowKey}`);
  await queueTable.deleteRow(rowKey);
}

async function getNextFromQueue(
  cnt = 1
): Promise<{ rowKey: string; url: string }[]> {
  console.error("Getting next queued item");

  const prefixUrl = `${SMOLTABLE_URL}/v1/table/${QUEUE_TABLE}/scan`;

  const response = await fetch(prefixUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      row: {
        prefix: "",
        limit: cnt,
      },
      column: {
        key: createColumnKey("url"),
      },
    }),
  });

  if (!response.ok) {
    throw new Error(
      `SMOLTABLE FAILED D: : ${response.status} ${await response.text()}`
    );
  }

  const data = (await response.json()) as {
    result: {
      rows: {
        row_key: string;
        columns: {
          url: {
            "": { time: number; value: string }[];
          };
        };
      }[];
    };
  };

  return data.result.rows.map((row) => ({
    rowKey: row.row_key,
    url: row.columns.url[""].at(0)!.value,
  }));
}

/**
 * Generates n-sized chunks from an array
 */
export function* sliceGenerator<T>(arr: T[], size: number): Generator<T[]> {
  let index = 0;
  let slice = arr.slice(index, index + size);
  while (slice.length) {
    yield slice;
    index += size;
    slice = arr.slice(index, index + size);
  }
}

async function enqueueSite(anchors: { href: string }[]): Promise<void> {
  console.log(`Enqueuing ${anchors.length} more URLs`);

  for (const slice of sliceGenerator(anchors, 5_000)) {
    await queueTable.write(
      slice.map(({ href }) => ({
        row_key: createId(),
        cells: [
          {
            column_key: createColumnKey("url"),
            type: "string",
            value: href,
          },
        ],
      }))
    );
  }
}

async function writeSite(
  prevSite: { formattedUrl: string; lang: string; title: string },
  anchors: { formattedUrl: string; text: string }[]
): Promise<void> {
  console.error(`Writing ${1 + anchors.length} items`);

  await mainTable.write([
    {
      row_key: prevSite.formattedUrl,
      cells: [
        {
          column_key: createColumnKey("language"),
          type: "string",
          value: prevSite.lang,
          time: 0,
        },
        {
          column_key: createColumnKey("title"),
          type: "string",
          value: prevSite.title,
          time: 0,
        },
      ],
    },
  ]);

  for (const slice of sliceGenerator(anchors, 5_000)) {
    await mainTable.write(
      slice.map(({ formattedUrl, text }) => ({
        row_key: formattedUrl,
        cells: [
          {
            column_key: createColumnKey("anchor", prevSite.formattedUrl),
            type: "string",
            value: text,
            time: 0,
          },
        ],
      }))
    );
  }
}

const blacklist = existsSync("blacklist.txt")
  ? readFileSync("blacklist.txt", "utf-8").split("\n").filter(Boolean)
  : [];

const HREF_BLACKLIST: RegExp[] = [
  /.pdf$/,
  /.exe$/,

  /^#/,
  /^\/w\//,
  /^\/api\//,

  /^tel:/,
  /^mailto:/,

  // https://en.wikipedia.org/wiki/Wikipedia:Namespace
  /^\/wiki\/Help:/,
  /^\/wiki\/Template:/,
  /^\/wiki\/Special:/,
  /^\/wiki\/Wikipedia:/,
  /^\/wiki\/User:/,
  /^\/wiki\/File:/,
  /^\/wiki\/MediaWiki:/,
  /^\/wiki\/Category:/,
  /^\/wiki\/Portal:/,
  /^\/wiki\/Draft:/,
  /^\/wiki\/TimedText:/,
  /^\/wiki\/Module:/,
  /^\/wiki\/Thread:/,
  /^\/wiki\/Summary:/,
  /^\/wiki\/Book:/,
  /^\/wiki\/Course:/,
  /^\/wiki\/Talk:/,
  /^\/wiki\/Template_talk:/,
];

function isBlacklisted(url: URL): boolean {
  return HREF_BLACKLIST.some((regex) => regex.test(url.pathname));
}

function reverseDomain(url: string): string {
  return new URL(url).host.split(".").reverse().join(".");
}

async function crawlSite(url: string, force = false): Promise<void> {
  if (blacklist.includes(url)) {
    console.log(`${url} is manually blacklisted`);
    return;
  }

  try {
    new URL(url);
  } catch (error) {
    console.error(`Unparseable URL: ${url}`);
    return;
  }

  const reversedDomain = reverseDomain(url);
  const formattedUrl = STORE_SUB_PAGES
    ? reversedDomain + new URL(url).pathname
    : reversedDomain;

  if (!force) {
    if (await siteAlreadyScraped(formattedUrl)) {
      console.log(`I already know ${formattedUrl}, not crawling that again`);
      return;
    }
  }

  console.log(`Crawling ${url}`);

  const response = await Promise.race([
    fetch(url, {
      redirect: "follow",
    }),
    new Promise<false>((resolve) => setTimeout(() => resolve(false), 5_000)),
  ]);

  if (!response) {
    console.log(`${url} was too slow`);
    blacklist.push(url);
    await appendFile("blacklist.txt", `${url}\n`);
    return;
  }

  if (!response.headers.get("content-type")?.includes("html")) {
    console.log(`${url} is not HTML`);
    return;
  }

  if (response.ok) {
    const html = await response.text();
    const $ = cheerio(html);

    const lang = $("html").attr("lang") ?? "en";
    const title = $("head > title").text().trim();

    const anchors = Array.from($("a"))
      // Get anchor text and href
      .map((x) => ({
        text: $(x).text().trim(),
        href: $(x).attr("href")!,
      }))
      // Filter empty texts
      .filter(({ text, href }) => !!href && !!text)
      // Filter by blacklist
      .filter(({ href }) => !isBlacklisted(new URL(resolveUrl(url, href))))
      // Filter invalid hrefs
      .filter(({ href }) => {
        try {
          new URL(resolveUrl(url, href));
          return true;
        } catch (error) {
          return false;
        }
      })
      // Absolutify hrefs
      .map(({ text, href }) => ({
        text,
        href: resolveUrl(url, href),
      }))
      // Maybe stay on same page
      .filter(({ href }) => {
        // TODO: is this actually working? got some weird hrefs like "File_upload_wizard" in webtable

        if (STAY_ON_PAGE) {
          const domainSegments = reverseDomain(href).split(".");
          const urlPrefix = domainSegments.filter(Boolean).join(".");
          return reverseDomain(STAY_ON_PAGE).startsWith(urlPrefix);
        }
        return true;
      });

    // TODO: try to filter out links that point to the same href... just take the first one

    await writeSite(
      {
        lang,
        title,
        formattedUrl,
      },
      anchors.map(({ text, href }) => ({
        formattedUrl: STORE_SUB_PAGES
          ? reversedDomain + new URL(href).pathname
          : reversedDomain,
        text,
      }))
    );

    if (anchors.length) {
      await enqueueSite(anchors);
    }

    if (STORE_DOCUMENTS) {
      await storeDocument(formattedUrl, html);
    }
  } else {
    console.error(
      `Response failed with ${response.status} !!!!!!!!!!!!!!!!!!!!!!!!!`
    );

    if (response.status != 404) {
      blacklist.push(url);
      await appendFile("blacklist.txt", `${url}\n`);
    }
  }
}

const mainTable = new Smoltable(MAIN_TABLE);
await mainTable.create();
await mainTable.createColumnFamilies({
  column_families: [
    {
      name: "title",
    },
    {
      name: "language",
    },
  ],
});
await mainTable.createColumnFamilies({
  column_families: [
    {
      name: "anchor",
    },
  ],
  locality_group: true,
});
await mainTable.createColumnFamilies({
  column_families: [
    {
      name: "contents",
      gc_settings: {
        version_limit: 10,
      },
    },
  ],
  locality_group: true,
});

const queueTable = new Smoltable(QUEUE_TABLE);
await queueTable.create();
await queueTable.createColumnFamilies({
  column_families: [
    {
      name: "url",
    },
  ],
});

let page = 0;

for (let i = 0; i < LIMIT; i++) {
  try {
    const head = await getNextFromQueue(PARALLELISM);

    if (head.length) {
      await Promise.all(
        head.map(async ({ rowKey, url }) => {
          try {
            await crawlSite(url);
          } catch (error) {
            console.error("CRAWL ERROR", error);
          }
          await removeSiteFromQueue(rowKey);
        })
      );
    } else {
      console.error("=== ENTRY POINT");
      //const STARTING_POINT = `https://news.ycombinator.com/?p=${page++}`;
      await crawlSite(ENTRY_POINT, true);
    }
  } catch (error) {
    console.error("LOOP ERROR", error);
  }
}

process.exit(0);
