import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';

// Source inspection complements ORM execution tests; it does not prove that
// every declared mapping participates in a real query.
function argumentsAt(source: string, opening: number) {
    const result: string[] = [];
    let start = opening + 1;
    let depth = 1;
    let quoted = false;
    for (let i = start; i < source.length; i++) {
        const character = source[i];
        if (quoted) {
            if (character === '\\') i++;
            else if (character === '"') quoted = false;
            continue;
        }
        if (character === '"') { quoted = true; continue; }
        if (character === '(' || character === '{' || character === '[') depth++;
        if (character === ')' || character === '}' || character === ']') depth--;
        if (depth === 0) {
            result.push(source.slice(start, i).trim());
            return result;
        }
        if (character === ',' && depth === 1) {
            result.push(source.slice(start, i).trim());
            start = i + 1;
        }
    }
    throw new Error('Unterminated entity macro');
}

const db = new Bun.SQL(databaseUrl);
const findings: { file: string; entity: string; column: string; issue: string }[] = [];
const defaults: { file: string; entity: string; column: string; type: string; expression: string; actual: string }[] = [];
let entities = 0;
let checkedColumns = 0;
try {
    const columns = await db`
        SELECT table_name, column_name, is_nullable, column_default,
               character_maximum_length, data_type, udt_name, numeric_precision, numeric_scale
        FROM information_schema.columns WHERE table_schema = 'public'`;
    const catalog = new Map(columns.map((row: any) => [`${row.table_name}.${row.column_name}`, row]));
    const physicalTypes = await db`
        SELECT c.relname AS table_name, a.attname AS column_name,
               format_type(a.atttypid,a.atttypmod) AS sql_type
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped`;
    const sqlTypes = new Map(physicalTypes.map((row: any) => [`${row.table_name}.${row.column_name}`, row.sql_type]));
    const primaryRows = await db`
        SELECT k.table_name, k.column_name FROM information_schema.key_column_usage k
        JOIN information_schema.table_constraints c
          ON c.constraint_catalog=k.constraint_catalog AND c.constraint_schema=k.constraint_schema
         AND c.constraint_name=k.constraint_name AND c.table_name=k.table_name
        WHERE c.table_schema='public' AND c.constraint_type='PRIMARY KEY'`;
    const primaryKeys = new Set(primaryRows.map((row: any) => `${row.table_name}.${row.column_name}`));
    const inferredTypes: Record<string, string> = {
        'std::pmr::string': 'text', 'ruvia::string': 'text', bool: 'boolean',
        'std::int16_t': 'small_int', 'std::int32_t': 'integer', 'std::int64_t': 'big_int',
        float: 'real', double: 'double_value',
    };
    const databaseTypes: Record<string, string> = {
        text: 'text', varchar: 'varchar', int2: 'small_int', int4: 'integer', int8: 'big_int',
        bool: 'boolean', float4: 'real', float8: 'double_value', numeric: 'numeric',
        uuid: 'uuid', json: 'json', jsonb: 'jsonb', inet: 'inet',
        timestamptz: 'timestamp_tz', timestamp: 'timestamp', date: 'date', bytea: 'bytea',
    };
    for await (const file of new Bun.Glob('service/**/*.entity.h').scan('.')) {
        const source = await Bun.file(file).text();
        const macros = source.matchAll(/\bRUVIA_DB_ENTITY\s*\(/g);
        for (const macro of macros) {
            const args = argumentsAt(source, macro.index! + macro[0].length - 1);
            const entity = args[0];
            const table = /^"([^"]+)"$/.exec(args[1])?.[1];
            assert(table, `Unsupported table expression: ${file} ${entity}`);
            entities++;
            for (const field of args.slice(2)) {
                if (!field.startsWith('RUVIA_DB_COLUMN(')) continue;
                const [column, valueType, options = ''] = argumentsAt(field, field.indexOf('('));
                const actual: any = catalog.get(`${table}.${column}`);
                const issue = (message: string) => findings.push({ file, entity, column, issue: message });
                if (!actual) { issue(`missing physical column ${table}.${column}`); continue; }
                checkedColumns++;
                if (primaryKeys.has(`${table}.${column}`) !== /\.primary_key_\s*=\s*true/.test(options))
                    issue(`primary key differs: database=${primaryKeys.has(`${table}.${column}`)}`);
                const entityType = /\.data_type_\s*=\s*ruvia::db_data_type::(\w+)/.exec(options)?.[1]
                    ?? inferredTypes[valueType];
                const databaseType = databaseTypes[actual.udt_name];
                if (databaseType && entityType !== databaseType)
                    issue(`data type differs: database=${databaseType}, entity=${entityType ?? valueType}`);
                else if (!databaseType && actual.data_type !== 'USER-DEFINED')
                    issue(`unsupported physical type audit: ${actual.udt_name}`);
                if (actual.udt_name === 'numeric' && actual.numeric_precision !== null) {
                    const precision = Number(/\.precision_\s*=\s*(\d+)/.exec(options)?.[1] ?? 0);
                    const scale = Number(/\.scale_\s*=\s*(\d+)/.exec(options)?.[1] ?? 0);
                    if (precision !== Number(actual.numeric_precision) || scale !== Number(actual.numeric_scale))
                        issue(`numeric precision differs: database=${actual.numeric_precision},${actual.numeric_scale}`);
                }
                if ((actual.is_nullable === 'YES') !== /\.nullable_\s*=\s*true/.test(options))
                    issue(`nullable differs: database=${actual.is_nullable}`);
                if (actual.column_default !== null && !/\.default_expression_\s*=/.test(options)
                    && !/\.generated_\s*=\s*true/.test(options))
                    issue(`missing default metadata: ${actual.column_default}`);
                const declaredDefault = /\.default_expression_\s*=\s*ruvia::fixed_string\{\s*("(?:\\.|[^"\\])*")\s*\}/.exec(options);
                if (declaredDefault) {
                    const expression: string = JSON.parse(declaredDefault[1]);
                    if (actual.column_default === null) issue(`unexpected default metadata: ${expression}`);
                    else defaults.push({ file, entity, column, expression, actual: actual.column_default,
                        type: String(sqlTypes.get(`${table}.${column}`)) });
                } else if (/\.default_expression_\s*=/.test(options)) {
                    issue('unsupported default expression declaration');
                }
                if (actual.character_maximum_length !== null) {
                    const length = Number(/\.length_\s*=\s*(\d+)/.exec(options)?.[1] ?? 0);
                    if (length !== Number(actual.character_maximum_length))
                        issue(`length differs: database=${actual.character_maximum_length}, entity=${length}`);
                }
                if (actual.data_type === 'USER-DEFINED' && !options.includes(`"${actual.udt_name}"`))
                    issue(`missing named type metadata: ${actual.udt_name}`);
            }
        }
    }
    // Parse defaults using the real physical type. No values are inserted and
    // no default function is executed. The temporary table disappears at commit.
    const unique = [...new Map(defaults.map(value => [JSON.stringify([value.type, value.expression]), value])).values()];
    const canonical = new Map<string, string>();
    if (unique.length) await db.begin(async (tx) => {
        await tx.unsafe(`CREATE TEMP TABLE entity_default_audit (${unique.map((value, i) =>
            `c${i} ${value.type} DEFAULT (${value.expression})`).join(',')}) ON COMMIT DROP`);
        const normalized = await tx`
            SELECT a.attname, pg_get_expr(d.adbin,d.adrelid) AS expression
            FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
            WHERE a.attrelid='pg_temp.entity_default_audit'::regclass`;
        for (const value of normalized) {
            const original = unique[Number(value.attname.slice(1))];
            canonical.set(JSON.stringify([original.type, original.expression]), value.expression);
        }
    });
    for (const value of defaults) {
        const normalized = canonical.get(JSON.stringify([value.type, value.expression]));
        if (normalized !== value.actual) findings.push({ file: value.file, entity: value.entity,
            column: value.column, issue: `default expression differs: database=${value.actual}, entity=${normalized}` });
    }
    const report = { entities, checkedColumns, checkedDefaults: defaults.length, findings };
    await Bun.write('build/entity-schema-audit.json', JSON.stringify(report, null, 2));
    console.log(`Audited ${entities} entities / ${checkedColumns} columns; ${findings.length} findings`);
    for (const finding of findings.slice(0, 12)) console.log(JSON.stringify(finding));
    assert.equal(findings.length, 0, 'Entity mapping gaps: see build/entity-schema-audit.json');
} finally {
    await db.close();
}
