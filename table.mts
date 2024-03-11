const SMOLTABLE_URL = "http://localhost:9876";

export function createColumnKey(family: string, column?: string): string {
  return column ? `${family}:${column}` : family;
}

type CellValue =
  | {
      type: "string";
      value: string;
    }
  | {
      type: "i32" | "i64" | "f32" | "f64" | "byte";
      value: number;
    }
  | {
      type: "boolean";
      value: boolean;
    };

export class Smoltable {
  name: string;

  constructor(name: string) {
    this.name = name;
  }

  // TODO: dedicated cache size:
  // TODO: queue -> 0
  // TODO: webtable -> 64mb?

  public async create(): Promise<void> {
    await fetch(`${SMOLTABLE_URL}/v1/table/${this.name}`, { method: "PUT" });
  }

  public async createColumnFamilies(input: {
    locality_group?: boolean;
    column_families: {
      name: string;
      gc_settings?: {
        version_limit?: number;
        ttl_secs?: number;
      };
    }[];
  }) {
    const res = await fetch(
      `${SMOLTABLE_URL}/v1/table/${this.name}/column-family`,
      {
        method: "POST",
        body: JSON.stringify(input),
        headers: {
          "Content-Type": "application/json",
        },
      }
    );

    if (!res.ok) {
      if (res.status != 409) {
        throw new Error(
          `SMOLTABLE FAILED WITH ${res.status}: ${await res.text()}`
        );
      }
    }
  }

  // TODO: return cell count
  public async deleteRow(rowKey: string): Promise<void> {
    const deleteUrl = `${SMOLTABLE_URL}/v1/table/${this.name}/row`;

    const response = await fetch(deleteUrl, {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        row_key: rowKey,
      }),
    });

    if (!response.ok) {
      throw new Error(
        `SMOLTABLE FAILED D: : ${response.status} ${await response.text()}`
      );
    }
  }

  public async write(
    items: {
      row_key: string;
      cells: ({
        column_key: string;
        time?: number;
      } & CellValue)[];
    }[]
  ) {
    const writeUrl = `${SMOLTABLE_URL}/v1/table/${this.name}/write`;

    const response = await fetch(writeUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        items,
      }),
    });

    if (!response.ok) {
      throw new Error(
        `SMOLTABLE FAILED D: : ${response.status} ${await response.text()}`
      );
    }
  }
}
