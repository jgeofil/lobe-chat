import { sql } from 'drizzle-orm';
import { and, eq, inArray } from 'drizzle-orm/expressions';

import * as EXPORT_TABLES from '@/database/schemas';
import { LobeChatDatabase } from '@/database/type';
import { ExportPgDataStructure } from '@/types/export';
import { ImportResultData, ImporterEntryData } from '@/types/importer';
import { uuid } from '@/utils/uuid';

import { DeprecatedDataImporterRepos } from './deprecated';

interface ImportResult {
  added: number;
  conflictFields?: string[];
  errors: number;
  skips: number;
  updated?: number;
}

// 定义表导入配置接口
interface TableImportConfig {
  // 冲突处理策略
  conflictStrategy?: 'skip' | 'modify' | 'merge';
  // 字段处理函数
  fieldProcessors?: {
    [field: string]: (value: any) => any;
  };
  // 是否使用复合主键（没有单独的id字段）
  isCompositeKey?: boolean;
  // 是否保留原始ID
  preserveId?: boolean;
  // 关系字段定义
  relations?: {
    field: string;
    sourceField?: string;
    sourceTable: string;
  }[];
  // 是否有自循环引用（如消息的parentId引用自身表）
  selfReferences?: {
    field: string;
    sourceField?: string;
  }[];
  // 表名
  table: string;
  // 表类型
  type: 'base' | 'relation';
  // 唯一约束字段
  uniqueConstraints?: string[];
}

// 导入表配置
const IMPORT_TABLE_CONFIG: TableImportConfig[] = [
  {
    conflictStrategy: 'merge',
    preserveId: true,
    // 特殊表，ID与用户ID相同
    table: 'userSettings',
    type: 'base',
    uniqueConstraints: ['id'],
  },
  {
    conflictStrategy: 'merge',
    isCompositeKey: true,
    table: 'userInstalledPlugins',
    type: 'base',
    uniqueConstraints: ['identifier'],
  },
  {
    conflictStrategy: 'skip',
    preserveId: true,
    table: 'aiProviders',
    type: 'base',
    uniqueConstraints: ['id'],
  },
  {
    conflictStrategy: 'skip',
    preserveId: true, // 需要保留原始ID
    relations: [
      {
        field: 'providerId',
        sourceTable: 'aiProviders',
      },
    ],
    table: 'aiModels',
    type: 'relation',
    uniqueConstraints: ['id', 'providerId'],
  },
  {
    table: 'sessionGroups',
    type: 'base',
    uniqueConstraints: [],
  },
  {
    // 对slug字段进行特殊处理
    fieldProcessors: {
      slug: (value) => `${value}-${uuid().slice(0, 8)}`,
    },
    relations: [
      {
        field: 'groupId',
        sourceTable: 'sessionGroups',
      },
    ],
    table: 'sessions',
    type: 'relation',
    uniqueConstraints: ['slug'],
  },
  {
    relations: [
      {
        field: 'sessionId',
        sourceTable: 'sessions',
      },
    ],
    table: 'topics',
    type: 'relation',
  },
  {
    fieldProcessors: {
      slug: (value) => (value ? `${value}-${uuid().slice(0, 8)}` : null),
    },
    table: 'agents',
    type: 'base',
    uniqueConstraints: ['slug'],
  },
  {
    isCompositeKey: true, // 使用复合主键 [fileId, agentId, userId]
    relations: [
      {
        field: 'agentId',
        sourceTable: 'agents',
      },
      {
        field: 'fileId',
        sourceTable: 'files',
      },
    ],
    table: 'agentsFiles',
    type: 'relation',
  },
  {
    isCompositeKey: true, // 使用复合主键 [agentId, knowledgeBaseId]
    relations: [
      {
        field: 'agentId',
        sourceTable: 'agents',
      },
      {
        field: 'knowledgeBaseId',
        sourceTable: 'knowledgeBases',
      },
    ],
    table: 'agentsKnowledgeBases',
    type: 'relation',
  },
  {
    isCompositeKey: true, // 使用复合主键 [agentId, sessionId]
    relations: [
      {
        field: 'agentId',
        sourceTable: 'agents',
      },
      {
        field: 'sessionId',
        sourceTable: 'sessions',
      },
    ],
    table: 'agentsToSessions',
    type: 'relation',
  },
  {
    relations: [
      {
        field: 'topicId',
        sourceTable: 'topics',
      },
    ],
    // 自引用关系处理
    selfReferences: [
      {
        field: 'parentThreadId',
      },
    ],
    table: 'threads',
    type: 'relation',
  },
  {
    relations: [
      {
        field: 'sessionId',
        sourceTable: 'sessions',
      },
      {
        field: 'topicId',
        sourceTable: 'topics',
      },
      {
        field: 'agentId',
        sourceTable: 'agents',
      },
      {
        field: 'threadId',
        sourceTable: 'threads',
      },
    ],
    // 自引用关系处理
    selfReferences: [
      {
        field: 'parentId',
      },
      {
        field: 'quotaId',
      },
    ],
    table: 'messages',
    type: 'relation',
  },
  {
    conflictStrategy: 'skip',
    preserveId: true, // 使用消息ID作为主键
    relations: [
      {
        field: 'id',
        sourceTable: 'messages',
      },
    ],
    table: 'messagePlugins',
    type: 'relation',
  },
  {
    isCompositeKey: true, // 使用复合主键 [messageId, chunkId]
    relations: [
      {
        field: 'messageId',
        sourceTable: 'messages',
      },
      {
        field: 'chunkId',
        sourceTable: 'chunks',
      },
    ],
    table: 'messageChunks',
    type: 'relation',
  },
  {
    isCompositeKey: true, // 使用复合主键 [id, queryId, chunkId]
    relations: [
      {
        field: 'id',
        sourceTable: 'messages',
      },
      {
        field: 'queryId',
        sourceTable: 'messageQueries',
      },
      {
        field: 'chunkId',
        sourceTable: 'chunks',
      },
    ],
    table: 'messageQueryChunks',
    type: 'relation',
  },
  {
    relations: [
      {
        field: 'messageId',
        sourceTable: 'messages',
      },
      {
        field: 'embeddingsId',
        sourceTable: 'embeddings',
      },
    ],
    table: 'messageQueries',
    type: 'relation',
  },
  {
    conflictStrategy: 'skip',
    preserveId: true, // 使用消息ID作为主键
    relations: [
      {
        field: 'id',
        sourceTable: 'messages',
      },
    ],
    table: 'messageTranslates',
    type: 'relation',
  },
  {
    conflictStrategy: 'skip',
    preserveId: true, // 使用消息ID作为主键
    relations: [
      {
        field: 'id',
        sourceTable: 'messages',
      },
      {
        field: 'fileId',
        sourceTable: 'files',
      },
    ],
    table: 'messageTts',
    type: 'relation',
  },
];

export class DataImporterRepos {
  private userId: string;
  private db: LobeChatDatabase;
  private deprecatedDataImporterRepos: DeprecatedDataImporterRepos;
  private idMaps: Record<string, Record<string, string>> = {};
  private selfReferenceMap: Record<
    string,
    { field: string; records: any[]; sourceField?: string }[]
  > = {};
  private conflictRecords: Record<string, { field: string; value: any }[]> = {};

  constructor(db: LobeChatDatabase, userId: string) {
    this.userId = userId;
    this.db = db;
    this.deprecatedDataImporterRepos = new DeprecatedDataImporterRepos(db, userId);
  }

  importData = async (
    data: ImporterEntryData | ExportPgDataStructure,
  ): Promise<ImportResultData> => {
    if ('schemaHash' in data) {
      return this.importPgData(data);
    }

    const results = await this.deprecatedDataImporterRepos.importData(data);
    return { results, success: true };
  };

  /**
   * 导入PostgreSQL数据
   */
  async importPgData(dbData: ExportPgDataStructure): Promise<ImportResultData> {
    const results: Record<string, ImportResult> = {};
    const { data } = dbData;

    // 初始化ID映射表和冲突记录
    this.idMaps = {};
    this.selfReferenceMap = {};
    this.conflictRecords = {};

    try {
      // 预先判断是否存在clientId重复问题
      await this.checkClientIdConflicts(data);

      await this.db.transaction(async (trx) => {
        // 按配置顺序导入表
        for (const config of IMPORT_TABLE_CONFIG) {
          const { table: tableName } = config;

          // @ts-ignore
          const tableData = data[tableName];

          if (!tableData || tableData.length === 0) {
            results[tableName] = { added: 0, errors: 0, skips: 0 };
            continue;
          }

          console.log(
            `Importing table: ${tableName}(${config.type}), records: ${tableData.length}`,
          );

          // 根据表类型选择导入方法
          if (config.type === 'base') {
            results[tableName] = await this.importTableData(trx, config, tableData);
          } else {
            results[tableName] = await this.importRelationTableData(trx, config, tableData);
          }
        }

        // 处理自引用关系
        await this.processAllSelfReferences(trx);
      });

      return {
        conflictRecords: this.conflictRecords,
        results,
        success: true,
      };
    } catch (error) {
      console.error('Import failed:', error);
      return {
        conflictRecords: this.conflictRecords,
        error: {
          details: this.extractErrorDetails(error),
          message: error.message,
          stack: error.stack,
        },
        results,
        success: false,
      };
    }
  }

  /**
   * 预检查clientId是否存在冲突
   */
  private async checkClientIdConflicts(data: any) {
    for (const config of IMPORT_TABLE_CONFIG) {
      const { table: tableName } = config;

      // @ts-ignore
      const tableData = data[tableName];
      if (!tableData || tableData.length === 0) continue;

      // @ts-ignore
      const table = EXPORT_TABLES[tableName];

      // 只检查有clientId字段的表
      if (!('clientId' in table) || !('userId' in table)) continue;

      const clientIds = tableData.filter((item) => item.clientId).map((item) => item.clientId);

      if (clientIds.length === 0) continue;

      // 查找当前用户下是否已存在相同clientId的记录
      // @ts-expect-error
      const existingRecords = await this.db.query[tableName].findMany({
        where: and(eq(table.userId, this.userId), inArray(table.clientId, clientIds)),
      });

      // 存储已存在记录的ID映射
      if (!this.idMaps[tableName]) {
        this.idMaps[tableName] = {};
      }

      for (const record of existingRecords) {
        // 对于每条已存在的记录，建立clientId到id的映射
        this.idMaps[tableName][record.clientId] = record.id;

        // 同时也建立id到id的映射（用于处理自引用）
        this.idMaps[tableName][record.id] = record.id;
      }
    }
  }

  /**
   * 从错误中提取详细信息
   */
  private extractErrorDetails(error: any) {
    if (error.code === '23505') {
      // PostgreSQL 唯一约束错误码
      const match = error.detail?.match(/Key \((.+?)\)=\((.+?)\) already exists/);
      if (match) {
        return {
          constraintType: 'unique',
          field: match[1],
          value: match[2],
        };
      }
    }

    return error.detail || 'Unknown error details';
  }

  /**
   * 处理所有表的自引用关系
   */
  private async processAllSelfReferences(trx: any) {
    // eslint-disable-next-line guard-for-in
    for (const tableName in this.selfReferenceMap) {
      // @ts-ignore
      const table = EXPORT_TABLES[tableName];

      for (const refInfo of this.selfReferenceMap[tableName]) {
        const { field, records } = refInfo;

        // 生成CASE WHEN语句更新自引用
        const updates = records
          .filter((record) => record.oldValue !== null && record.oldValue !== undefined)
          .map((record) => {
            const newValue = this.idMaps[tableName][record.oldValue];
            if (newValue) {
              return sql`WHEN ${table.id} = ${record.id} THEN ${newValue}`;
            }
            return null;
          })
          .filter(Boolean);

        if (updates.length > 0) {
          // 执行批量更新
          await trx
            .update(table)
            .set({
              [field]: sql`CASE ${sql.join(updates)} ELSE ${table[field]} END`,
            })
            .where(
              inArray(
                table.id,
                records.map((r) => r.id),
              ),
            );

          console.log(`Updated ${updates.length} self-references for ${tableName}.${field}`);
        }
      }
    }
  }

  /**
   * 导入基础表数据
   */
  private async importTableData(
    trx: any,
    config: TableImportConfig,
    tableData: any[],
  ): Promise<ImportResult> {
    const {
      table: tableName,
      preserveId,
      isCompositeKey = false,
      uniqueConstraints = [],
      conflictStrategy = 'modify',
      fieldProcessors = {},
    } = config;

    // @ts-ignore
    const table = EXPORT_TABLES[tableName];
    const result: ImportResult = { added: 0, conflictFields: [], errors: 0, skips: 0, updated: 0 };

    try {
      // 初始化该表的ID映射
      if (!this.idMaps[tableName]) {
        this.idMaps[tableName] = {};
      }

      // 1. 查找已存在的记录（基于clientId和userId）
      let existingRecords: any[] = [];

      if ('clientId' in table && 'userId' in table) {
        const clientIds = tableData
          .filter((item) => item.clientId)
          .map((item) => item.clientId)
          .filter(Boolean);

        if (clientIds.length > 0) {
          existingRecords = await trx.query[tableName].findMany({
            where: and(eq(table.userId, this.userId), inArray(table.clientId, clientIds)),
          });
        }
      }

      // 如果需要保留原始ID，还需要检查ID是否已存在
      if (preserveId && !isCompositeKey) {
        const ids = tableData.map((item) => item.id).filter(Boolean);
        if (ids.length > 0) {
          const idExistingRecords = await trx.query[tableName].findMany({
            where: inArray(table.id, ids),
          });

          // 合并到已存在记录集合中
          existingRecords = [
            ...existingRecords,
            ...idExistingRecords.filter(
              (record) => !existingRecords.some((existing) => existing.id === record.id),
            ),
          ];
        }
      }

      result.skips = existingRecords.length;

      // 2. 为已存在的记录建立ID映射
      for (const record of existingRecords) {
        // 只有非复合主键表才需要ID映射
        if (!isCompositeKey) {
          this.idMaps[tableName][record.id] = record.id;
          if (record.clientId) {
            this.idMaps[tableName][record.clientId] = record.id;
          }
        }
      }

      // 3. 筛选出需要插入的记录
      const recordsToInsert = tableData.filter(
        (item) =>
          !existingRecords.some(
            (record) =>
              (record.clientId === item.clientId && record.clientId) ||
              (preserveId && !isCompositeKey && record.id === item.id),
          ),
      );

      if (recordsToInsert.length === 0) {
        return result;
      }

      // 4. 准备导入数据
      const preparedData = recordsToInsert.map((item) => {
        const originalId = item.id;

        // 处理日期字段
        const dateFields: any = {};
        if (item.createdAt) dateFields.createdAt = new Date(item.createdAt);
        if (item.updatedAt) dateFields.updatedAt = new Date(item.updatedAt);
        if (item.accessedAt) dateFields.accessedAt = new Date(item.accessedAt);

        // 特殊处理：根据preserveId决定是否保留原始ID
        const newItem = {
          ...(preserveId && !isCompositeKey ? item : { ...item, id: undefined }),
          clientId: item.clientId || item.id,
          userId: this.userId,
          ...dateFields,
        };

        // 特殊表处理
        if (tableName === 'userSettings') {
          newItem.id = this.userId;
        }

        return { newItem, originalId };
      });

      // 5. 检查唯一约束并应用冲突策略
      for (const record of preparedData) {
        // 处理唯一约束
        for (const field of uniqueConstraints) {
          if (!record.newItem[field]) continue;

          // 检查字段值是否已存在
          const exists = await trx.query[tableName].findFirst({
            where: eq(table[field], record.newItem[field]),
          });

          if (exists) {
            // 记录冲突
            if (!this.conflictRecords[tableName]) this.conflictRecords[tableName] = [];
            this.conflictRecords[tableName].push({
              field,
              value: record.newItem[field],
            });

            // 应用冲突策略
            switch (conflictStrategy) {
              case 'skip': {
                record.newItem._skip = true;
                result.skips++;
                break;
              }
              case 'modify': {
                // 应用字段处理器
                if (field in fieldProcessors) {
                  record.newItem[field] = fieldProcessors[field](record.newItem[field]);
                }
                break;
              }
              case 'merge': {
                console.log(field, record.newItem[field], table[field], record);
                // 合并数据
                const mergeData = await trx
                  .update(tableName)
                  .set(record.newItem)
                  .where(eq(table[field], record.newItem[field]));
                console.log('mergeData:', mergeData);
                record.newItem._skip = true;
                result.updated++;
                break;
              }
            }
          }
        }
      }

      // 过滤掉标记为跳过的记录
      const filteredData = preparedData.filter((record) => !record.newItem._skip);

      // 清除临时标记
      filteredData.forEach((record) => delete record.newItem._skip);

      // 6. 批量插入数据
      const BATCH_SIZE = 100;

      for (let i = 0; i < filteredData.length; i += BATCH_SIZE) {
        const batch = filteredData.slice(i, i + BATCH_SIZE).filter(Boolean);

        const itemsToInsert = batch.map((item) => item.newItem);
        const originalIds = batch.map((item) => item.originalId);
        console.log('itemsToInsert:', itemsToInsert);

        try {
          // 插入并返回结果
          const res = await trx.insert(table).values(itemsToInsert).returning();
          const insertResult = res.map((item) => ({
            clientId: item?.clientId || undefined,
            id: isCompositeKey ? undefined : item?.id,
          }));

          result.added += insertResult.length;

          // 建立ID映射关系 (只对非复合主键表)
          if (!isCompositeKey) {
            for (const [j, newRecord] of insertResult.entries()) {
              const originalId = originalIds[j];
              this.idMaps[tableName][originalId] = newRecord.id;

              // 非常重要: 同时也将clientId映射到新ID (解决问题2)
              if (newRecord.clientId) {
                this.idMaps[tableName][newRecord.clientId] = newRecord.id;
              }
            }
          }
        } catch (error) {
          console.error(`Error batch inserting ${tableName}:`, error);

          // 处理错误并记录
          if (error.code === '23505') {
            const match = error.detail?.match(/Key \((.+?)\)=\((.+?)\) already exists/);
            if (match) {
              const conflictField = match[1];
              if (!result.conflictFields) result.conflictFields = [];
              if (!result.conflictFields.includes(conflictField)) {
                result.conflictFields.push(conflictField);
              }

              if (!this.conflictRecords[tableName]) this.conflictRecords[tableName] = [];
              this.conflictRecords[tableName].push({
                field: conflictField,
                value: match[2],
              });
            }
          }

          result.errors += batch.length;
        }
      }

      return result;
    } catch (error) {
      console.error(`Error importing table ${tableName}:`, error);
      result.errors = tableData.length;
      return result;
    }
  }

  /**
   * 导入关系表数据
   */
  private async importRelationTableData(
    trx: any,
    config: TableImportConfig,
    tableData: any[],
  ): Promise<ImportResult> {
    const {
      table: tableName,
      relations = [],
      selfReferences = [],
      preserveId,
      isCompositeKey = false,
      uniqueConstraints = [],
      conflictStrategy = 'modify',
      fieldProcessors = {},
    } = config;

    // @ts-ignore
    const table = EXPORT_TABLES[tableName];
    const result: ImportResult = { added: 0, conflictFields: [], errors: 0, skips: 0 };

    try {
      if (!this.idMaps[tableName]) {
        this.idMaps[tableName] = {};
      }

      // 初始化自引用映射
      if (selfReferences.length > 0) {
        this.selfReferenceMap[tableName] = selfReferences.map((ref) => ({
          ...ref,
          records: [],
        }));
      }

      // 1. 查找已存在的记录
      let existingRecords: any[] = [];

      if ('clientId' in table && 'userId' in table) {
        const clientIds = tableData.filter((item) => item.clientId).map((item) => item.clientId);

        if (clientIds.length > 0) {
          existingRecords = await trx.query[tableName].findMany({
            where: and(eq(table.userId, this.userId), inArray(table.clientId, clientIds)),
          });
        }
      }

      result.skips = existingRecords.length;

      // 2. 为已存在的记录建立ID映射
      for (const record of existingRecords) {
        if (!isCompositeKey) {
          this.idMaps[tableName][record.id] = record.id;
          if (record.clientId) {
            this.idMaps[tableName][record.clientId] = record.id;
          }
        }
      }

      // 3. 筛选需要插入的记录
      const recordsToInsert = tableData.filter(
        (item) =>
          !existingRecords.some(
            (record) =>
              (record.clientId === item.clientId && record.clientId) ||
              (preserveId && !isCompositeKey && record.id === item.id),
          ),
      );

      if (recordsToInsert.length === 0) {
        return result;
      }

      // 4. 处理和替换外键ID
      const processedData = [];

      for (const record of recordsToInsert) {
        // 检查空记录
        if (!record || typeof record !== 'object') {
          console.warn(`Invalid record in table ${tableName}:`, record);
          result.skips++;
          continue;
        }

        const originalId = record.id;

        // 处理日期字段
        const dateFields: any = {};
        if (record.createdAt) dateFields.createdAt = new Date(record.createdAt);
        if (record.updatedAt) dateFields.updatedAt = new Date(record.updatedAt);
        if (record.accessedAt) dateFields.accessedAt = new Date(record.accessedAt);

        // 创建新记录对象
        let newRecord: any = {};

        // 对于复合主键表，不考虑id字段
        if (isCompositeKey) {
          // 不包含id字段，只复制其他字段
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          const { id: _, ...rest } = record;
          newRecord = {
            ...rest,
            ...dateFields,
            clientId: record.clientId || record.id,
            userId: this.userId,
          };
        } else {
          // 根据是否保留ID决定如何处理
          newRecord = {
            ...(preserveId ? record : { ...record, id: undefined }),
            ...dateFields,
            clientId: record.clientId || record.id,
            userId: this.userId,
          };
        }

        // 替换外键引用
        let skipRecord = false;

        // 处理关系字段
        for (const relation of relations) {
          const { field, sourceTable } = relation;

          if (newRecord[field] && this.idMaps[sourceTable]) {
            const mappedId = this.idMaps[sourceTable][newRecord[field]];

            if (mappedId) {
              newRecord[field] = mappedId;
            } else {
              console.warn(
                `Could not find mapped ID for ${field}=${newRecord[field]} in table ${sourceTable}`,
              );
              skipRecord = true;
              break;
            }
          }
        }

        // 收集自引用字段信息，暂时不处理自引用，等所有记录都插入后再处理
        for (const selfRef of selfReferences) {
          const { field } = selfRef;

          if (newRecord[field]) {
            // 保存原始值，稍后更新
            const refIndex = this.selfReferenceMap[tableName].findIndex((r) => r.field === field);

            // 设置为null，等所有记录都插入后再更新
            const oldValue = newRecord[field];
            newRecord[field] = null;

            // 我们需要保存记录ID以便稍后更新
            const refData = {
              field,
              id: undefined, // 插入后会更新这个值
              oldValue,
            };

            // 暂存引用信息
            if (refIndex >= 0) {
              this.selfReferenceMap[tableName][refIndex].records.push(refData);
            }
          }
        }

        if (!skipRecord) {
          processedData.push({ newRecord, originalId, selfReferences: selfReferences.length > 0 });
        } else {
          result.skips++;
        }
      }

      if (processedData.length === 0) {
        return result;
      }

      // 5. 检查唯一约束
      for (const record of processedData) {
        // 处理唯一约束
        for (const field of uniqueConstraints) {
          if (!record.newRecord[field]) continue;

          // 检查唯一约束
          const exists = await trx.query[tableName].findFirst({
            where: eq(table[field], record.newRecord[field]),
          });

          if (exists) {
            // 记录冲突
            if (!this.conflictRecords[tableName]) this.conflictRecords[tableName] = [];
            this.conflictRecords[tableName].push({
              field,
              value: record.newRecord[field],
            });

            // 应用冲突策略
            if (conflictStrategy === 'skip') {
              record.newRecord._skip = true;
              result.skips++;
            } else if (conflictStrategy === 'modify' && field in fieldProcessors) {
              // 应用字段处理器修改冲突字段
              record.newRecord[field] = fieldProcessors[field](record.newRecord[field]);
            }
          }
        }

        // 对于需要保留ID的表，检查ID是否已存在
        if (preserveId && !isCompositeKey && record.newRecord.id) {
          const idExists = await trx.query[tableName].findFirst({
            where: eq(table.id, record.newRecord.id),
          });

          if (idExists && conflictStrategy === 'skip') {
            record.newRecord._skip = true;
            result.skips++;

            // 记录ID映射，即使跳过也要维护映射关系
            this.idMaps[tableName][record.originalId] = record.newRecord.id;
          }
          // 其他策略: 'modify' 对于ID不适用，需要跳过
        }
      }

      // 过滤掉标记为跳过的记录
      const filteredData = processedData.filter((record) => !record.newRecord._skip);

      // 清除临时标记
      filteredData.forEach((record) => delete record.newRecord._skip);

      // 6. 批量插入处理后的数据
      const BATCH_SIZE = 100;

      for (let i = 0; i < filteredData.length; i += BATCH_SIZE) {
        const batch = filteredData.slice(i, i + BATCH_SIZE);
        const itemsToInsert = batch.map((item) => item.newRecord);
        const originalIds = batch.map((item) => item.originalId);
        const hasSelfRefs = batch.map((item) => item.selfReferences);

        try {
          // 插入记录
          const insertQuery = trx.insert(table).values(itemsToInsert);

          // 只对非复合主键的表需要返回ID
          let insertResult;
          if (!isCompositeKey) {
            insertResult = await insertQuery.returning({
              clientId: 'clientId' in table ? table.clientId : undefined,
              id: table.id,
            });
          } else {
            await insertQuery;
            insertResult = itemsToInsert.map(() => ({})); // 创建空结果以维持计数
          }

          result.added += insertResult.length;

          // 保存ID映射关系并更新自引用记录
          for (const [j, element] of insertResult.entries()) {
            if (!isCompositeKey) {
              const newId = element.id;
              const originalId = originalIds[j];

              // 保存ID映射
              this.idMaps[tableName][originalId] = newId;

              // 重要：如果有clientId，也建立clientId到新ID的映射
              if (element.clientId) {
                this.idMaps[tableName][element.clientId] = newId;
              }

              // 如果这个记录有自引用字段，更新自引用记录集合
              if (hasSelfRefs[j]) {
                for (const refMap of this.selfReferenceMap[tableName]) {
                  for (const refRecord of refMap.records) {
                    if (refRecord.oldValue === originalId) {
                      refRecord.id = newId; // 更新记录的ID
                    }
                  }
                }
              }
            }
          }
        } catch (error) {
          console.error(`Error batch inserting relation table ${tableName}:`, error);

          // 识别并记录约束冲突
          if (error.code === '23505') {
            if (!this.conflictRecords[tableName]) this.conflictRecords[tableName] = [];

            const match = error.detail?.match(/Key \((.+?)\)=\((.+?)\) already exists/);
            if (match) {
              const conflictField = match[1];
              if (!result.conflictFields) result.conflictFields = [];
              if (!result.conflictFields.includes(conflictField)) {
                result.conflictFields.push(conflictField);
              }

              this.conflictRecords[tableName].push({
                field: match[1],
                value: match[2],
              });
            }
          }

          result.errors += batch.length;
          // 继续处理其他批次
        }
      }

      return result;
    } catch (error) {
      console.error(`Error importing relation table ${tableName}:`, error);
      result.errors = tableData.length;
      return result;
    }
  }
}
