import { Context } from "moleculer";
export declare type BigQueryRegions = "us-central1" | "us-west4" | "us-west2" | "northamerica-northeast1" | "us-east4" | "us-west1" | "us-west3" | "southamerica-east1" | "us-east1" | "europe-west1" | "europe-north1" | "europe-west3" | "europe-west2" | "europe-west4" | "europe-central2" | "europe-west6" | "asia-east2" | "asia-southeast2" | "australia-southeast2" | "asia-south1" | "asia-northeast2" | "asia-northeast3" | "asia-southeast1" | "australia-southeast1" | "asia-east1" | "asia-northeast1";
export declare type BigQueryMultiRegions = "ASIA" | "US" | "EU";
export declare type BigQueryDbAdapterOptions = {
    getRegion: (ctx: Context) => Promise<BigQueryRegions | BigQueryMultiRegions>;
    getIdKey: (ctx?: Context) => Promise<string>;
    getTableName: (ctx?: BigQueryContext) => string;
    queryWrapper?: (query: string) => string;
    queryBlacklist?: Array<string>;
    projectId: string;
    showLogs?: boolean;
};
export declare type BigQueryContext = {
    org: string;
    impact: string;
    tableName?: string;
    region: string;
};
export interface TableSchemaFragment {
    table_catalog: string;
    table_schema: string;
    table_name: string;
    column_name: string;
    ordinal_position: number;
    is_nullable: string;
    data_type: string;
    is_hidden: string;
    is_system_defined: string;
    is_partitioning_column: string;
    clustering_ordinal_position: any;
}
