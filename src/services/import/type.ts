import { ExportPgDataStructure } from '@/types/export';
import { ImporterEntryData, OnImportCallbacks } from '@/types/importer';
import { UserSettings } from '@/types/user/settings';

export interface IImportService {
  importData(data: ImporterEntryData, callbacks?: OnImportCallbacks): Promise<void>;

  importPgData(
    data: ExportPgDataStructure,
    options?: {
      callbacks?: OnImportCallbacks;
      overwriteExisting?: boolean;
    },
  ): Promise<void>;

  importSettings(settings: UserSettings): Promise<void>;
}
