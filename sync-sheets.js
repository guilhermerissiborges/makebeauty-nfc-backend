// ========================================
// SINCRONIZAÇÃO GOOGLE SHEETS → MONGODB
// VERSÃO OTIMIZADA - À prova de falhas
// ========================================

const { google } = require('googleapis');
const mongoose = require('mongoose');
const crypto = require('crypto');
require('dotenv').config();

// ========================================
// CONECTAR AO MONGODB
// ========================================
console.log('🔄 Conectando ao MongoDB...');

mongoose.connect(process.env.MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  console.log('✅ Conectado ao MongoDB com sucesso!');
}).catch((err) => {
  console.error('❌ ERRO CRÍTICO: Falha ao conectar MongoDB:', err);
  process.exit(1);
});

// ========================================
// SCHEMA DO PRODUTO
// ========================================
const ProductSchema = new mongoose.Schema({
  nfcUID: { type: String, required: true, unique: true, index: true },
  productId: { type: String, required: true },
  productName: String,
  batchNumber: String,
  manufacturingDate: Date,
  manufacturingDateTime: Date,
  expiryDate: Date,
  manufacturingLocation: String,
  secretKey: String,
  scanCount: { type: Number, default: 0 },
  isActive: { type: Boolean, default: true },
  scans: [],
  createdAt: { type: Date, default: Date.now },
  syncedFromSheets: { type: Boolean, default: true },
  updatedAt: { type: Date, default: Date.now }
});

const Product = mongoose.model('Product', ProductSchema);

// ========================================
// FUNÇÕES AUXILIARES
// ========================================

/**
 * Normaliza UID removendo caracteres especiais
 */
function normalizeUID(uid) {
  if (!uid) return null;
  return uid.replace(/[:\s-]/g, '').toUpperCase().trim();
}

/**
 * Normaliza cabeçalhos de colunas
 */
function normalizeHeader(header) {
  return header.toLowerCase().replace(/[^a-z0-9]/g, '');
}

/**
 * Encontra coluna por múltiplos nomes possíveis
 */
function findColumn(headers, possibleNames) {
  const normalizedHeaders = headers.map(h => ({
    original: h,
    normalized: normalizeHeader(h)
  }));

  for (const name of possibleNames) {
    const normalized = normalizeHeader(name);
    const found = normalizedHeaders.find(h => h.normalized === normalized);
    if (found) return found.original;
  }
  
  return null;
}

/**
 * Converte string de data para objeto Date
 * Aceita múltiplos formatos
 */
function parseDate(dateString, isExpiry = false) {
  if (!dateString || dateString.trim() === '') return null;

  const trimmed = dateString.trim();

  // Formato ISO: YYYY-MM-DD HH:MM:SS
  if (trimmed.match(/^\d{4}-\d{2}-\d{2}/)) {
    const parts = trimmed.split(' ');
    const datePart = parts[0];
    const timePart = parts[1] || '00:00:00';
    
    const [year, month, day] = datePart.split('-');
    const [hour, minute, second] = timePart.split(':');
    
    return new Date(
      parseInt(year),
      parseInt(month) - 1,
      parseInt(day),
      parseInt(hour || 0),
      parseInt(minute || 0),
      parseInt(second || 0)
    );
  }

  // Formato: dd/mm/yyyy HH:mm:ss ou dd/mm/yyyy
  if (trimmed.includes('/')) {
    const parts = trimmed.split(' ');
    const datePart = parts[0];
    const timePart = parts[1] || '00:00:00';
    
    const dateSplit = datePart.split('/');
    
    if (dateSplit.length === 3) {
      // dd/mm/yyyy
      const [day, month, year] = dateSplit;
      const [hour, minute, second] = timePart.split(':');
      
      return new Date(
        parseInt(year),
        parseInt(month) - 1,
        parseInt(day),
        parseInt(hour || 0),
        parseInt(minute || 0),
        parseInt(second || 0)
      );
    } else if (dateSplit.length === 2) {
      // MM/AA ou MM/AAAA (validade)
      let [month, year] = dateSplit;
      
      if (year.length === 2) {
        year = '20' + year;
      }
      
      // Último dia do mês
      const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate();
      
      return new Date(
        parseInt(year),
        parseInt(month) - 1,
        lastDay,
        23, 59, 59
      );
    }
  }
  
  // Fallback: tentar parse direto
  const parsed = new Date(dateString);
  if (!isNaN(parsed.getTime())) {
    return parsed;
  }
  
  return null;
}

// ========================================
// CONFIGURAR GOOGLE SHEETS API
// ========================================
async function getGoogleSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: './credentials.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });
  
  return sheets;
}

// ========================================
// LER DADOS DO GOOGLE SHEETS
// ========================================
async function readGoogleSheet(spreadsheetId, range = 'Sheet1!A:E') {
  try {
    const sheets = await getGoogleSheetsClient();
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    
    if (!rows || rows.length === 0) {
      console.log('⚠️  Planilha vazia');
      return [];
    }

    // Primeira linha = cabeçalho
    const headers = rows[0];
    console.log('📋 Cabeçalhos encontrados:', headers);

    // Descobrir quais colunas são quais
    const uidCol = findColumn(headers, ['UID', 'uid', 'Uid', 'ID', 'id']);
    const dataCol = findColumn(headers, ['Data_Hora', 'DataHora', 'data_hora', 'Data', 'data', 'Fabricacao', 'fabricacao']);
    const nomeCol = findColumn(headers, ['Nome', 'nome', 'Produto', 'produto', 'Descricao', 'descricao']);
    const loteCol = findColumn(headers, ['Lote', 'lote', 'Batch', 'batch']);
    const validadeCol = findColumn(headers, ['Validade', 'validade', 'Expiry', 'expiry', 'Vencimento', 'vencimento']);

    console.log('\n🔍 Mapeamento de colunas:');
    console.log(`   UID: ${uidCol || '❌ NÃO ENCONTRADA'}`);
    console.log(`   Data/Hora: ${dataCol || '⚠️  Opcional'}`);
    console.log(`   Nome: ${nomeCol || '⚠️  Opcional'}`);
    console.log(`   Lote: ${loteCol || '⚠️  Opcional'}`);
    console.log(`   Validade: ${validadeCol || '⚠️  Opcional'}\n`);

    if (!uidCol) {
      console.error('❌ ERRO: Coluna UID não encontrada!');
      return [];
    }

    const data = [];

    // Converter linhas em objetos
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      
      const obj = {
        UID: uidCol ? row[headers.indexOf(uidCol)] : null,
        Data_Hora: dataCol ? row[headers.indexOf(dataCol)] : null,
        Nome: nomeCol ? row[headers.indexOf(nomeCol)] : null,
        Lote: loteCol ? row[headers.indexOf(loteCol)] : null,
        Validade: validadeCol ? row[headers.indexOf(validadeCol)] : null
      };
      
      if (obj.UID) {
        data.push(obj);
      }
    }

    return data;
  } catch (error) {
    console.error('❌ Erro ao ler Google Sheets:', error.message);
    throw error;
  }
}

// ========================================
// SINCRONIZAR COM MONGODB
// ========================================
async function syncSheetsToMongoDB(spreadsheetId) {
  try {
    console.log('\n📊 Lendo Google Sheets...');
    
    const data = await readGoogleSheet(spreadsheetId, 'Sheet1!A:E');
    
    console.log(`📋 Encontradas ${data.length} linhas válidas\n`);

    if (data.length === 0) {
      console.log('⚠️  Nenhum dado para sincronizar');
      return { novos: 0, atualizados: 0, erros: 0 };
    }

    let novos = 0;
    let atualizados = 0;
    let erros = 0;

    for (const row of data) {
      try {
        // Normalizar UID
        const rawUID = row.UID;
        const nfcUID = normalizeUID(rawUID);
        
        if (!nfcUID) {
          console.log('⚠️  Linha sem UID válido, pulando...');
          continue;
        }

        const dataHora = row.Data_Hora;
        const nomeProduto = row.Nome || 'Produto Make Beauty';
        const lote = row.Lote;
        const validade = row.Validade;
        
        // Converter data de fabricação
        let manufacturingDateTime;
        if (dataHora) {
          manufacturingDateTime = parseDate(dataHora);
        } else {
          manufacturingDateTime = new Date();
          console.log(`⚠️  UID ${nfcUID}: Sem data, usando data atual`);
        }
        
        if (!manufacturingDateTime || isNaN(manufacturingDateTime.getTime())) {
          console.log(`⚠️  Data inválida para ${nfcUID}: ${dataHora}`);
          manufacturingDateTime = new Date();
        }

        // Converter data de validade
        let expiryDate = null;
        if (validade && validade.trim() !== '') {
          expiryDate = parseDate(validade, true);
          
          if (expiryDate && !isNaN(expiryDate.getTime())) {
            if (!validade.includes(':')) {
              expiryDate.setHours(23, 59, 59);
            }
          } else {
            console.log(`⚠️  ${nfcUID}: Validade inválida, calculando +2 anos`);
            expiryDate = new Date(manufacturingDateTime);
            expiryDate.setFullYear(expiryDate.getFullYear() + 2);
          }
        } else {
          // Validade padrão: +2 anos
          expiryDate = new Date(manufacturingDateTime);
          expiryDate.setFullYear(expiryDate.getFullYear() + 2);
        }

        // Gerar lote se não informado
        const batchNumber = lote || ('LT' + manufacturingDateTime.toISOString().slice(0,10).replace(/-/g,''));

        // ID do produto
        const productId = `MB-${batchNumber}-${nfcUID}`;

        // Chave secreta (mesmo que não seja usada)
        const secretKey = crypto.randomBytes(32).toString('hex');
        const secretKeyHash = crypto.createHash('sha256').update(secretKey).digest('hex');

        // Verificar se existe
        const existingProduct = await Product.findOne({ nfcUID });

        if (existingProduct) {
          // Atualizar apenas se mudou
          let updated = false;
          
          if (existingProduct.productName !== nomeProduto) {
            existingProduct.productName = nomeProduto;
            updated = true;
          }
          
          if (existingProduct.batchNumber !== batchNumber) {
            existingProduct.batchNumber = batchNumber;
            updated = true;
          }
          
          if (existingProduct.manufacturingDateTime?.getTime() !== manufacturingDateTime.getTime()) {
            existingProduct.manufacturingDateTime = manufacturingDateTime;
            existingProduct.manufacturingDate = new Date(manufacturingDateTime.toDateString());
            updated = true;
          }
          
          if (existingProduct.expiryDate?.getTime() !== expiryDate?.getTime()) {
            existingProduct.expiryDate = expiryDate;
            updated = true;
          }
          
          existingProduct.syncedFromSheets = true;
          existingProduct.updatedAt = new Date();
          
          if (updated) {
            await existingProduct.save();
            console.log(`🔄 Atualizado: ${nfcUID} - ${nomeProduto}`);
            atualizados++;
          } else {
            console.log(`✓ Sem alterações: ${nfcUID}`);
          }
        } else {
          // Criar novo
          const newProduct = new Product({
            nfcUID,
            productId,
            productName: nomeProduto,
            batchNumber: batchNumber,
            manufacturingDate: new Date(manufacturingDateTime.toDateString()),
            manufacturingDateTime,
            expiryDate,
            manufacturingLocation: 'São Paulo, Brasil',
            secretKey: secretKeyHash,
            isActive: true,
            syncedFromSheets: true
          });

          await newProduct.save();
          
          console.log(`✅ Novo: ${nfcUID} - ${nomeProduto}`);
          console.log(`   Lote: ${batchNumber}`);
          console.log(`   Fabricação: ${manufacturingDateTime.toLocaleString('pt-BR')}`);
          console.log(`   Validade: ${expiryDate.toLocaleDateString('pt-BR')}`);
          
          novos++;
        }

      } catch (err) {
        console.error(`❌ Erro ao processar linha:`, err.message);
        erros++;
      }
    }

    // Resumo
    console.log('\n' + '='.repeat(60));
    console.log('📊 RESUMO DA SINCRONIZAÇÃO:');
    console.log('='.repeat(60));
    console.log(`✅ Novos produtos: ${novos}`);
    console.log(`🔄 Atualizados: ${atualizados}`);
    console.log(`❌ Erros: ${erros}`);
    console.log(`📋 Total processado: ${data.length}`);
    console.log('='.repeat(60) + '\n');

    return { novos, atualizados, erros };

  } catch (error) {
    console.error('❌ Erro na sincronização:', error);
    throw error;
  }
}

// ========================================
// EXECUTAR
// ========================================
const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_ID || process.argv[2];

if (!SPREADSHEET_ID) {
  console.error('❌ Erro: Forneça o ID da planilha');
  console.log('Uso: node sync-sheets.js ID_DA_PLANILHA');
  console.log('Ou configure GOOGLE_SHEETS_ID no .env');
  process.exit(1);
}

console.log('\n🚀 SINCRONIZAÇÃO TRUETOUCH™');
console.log('='.repeat(60));
console.log(`📊 Planilha ID: ${SPREADSHEET_ID}`);
console.log(`🔍 Procurando colunas: UID, Data_Hora, Nome, Lote, Validade`);
console.log('='.repeat(60) + '\n');

// Primeira sincronização
syncSheetsToMongoDB(SPREADSHEET_ID)
  .then((result) => {
    console.log('✅ Sincronização inicial concluída!');
  })
  .catch((error) => {
    console.error('❌ Falha na sincronização inicial:', error);
  });

// ========================================
// SINCRONIZAÇÃO AUTOMÁTICA
// ========================================
const INTERVALO_MINUTOS = parseInt(process.env.SYNC_INTERVAL_MINUTES) || 2;

console.log(`⏰ Modo automático ativado (a cada ${INTERVALO_MINUTOS} min)\n`);

setInterval(async () => {
  const timestamp = new Date().toLocaleString('pt-BR');
  console.log(`\n⏰ [${timestamp}] Executando sincronização agendada...`);
  
  try {
    await syncSheetsToMongoDB(SPREADSHEET_ID);
  } catch (error) {
    console.error('❌ Erro na sincronização agendada:', error.message);
  }
}, INTERVALO_MINUTOS * 60 * 1000);

// ========================================
// TRATAMENTO DE SINAIS
// ========================================
process.on('SIGTERM', async () => {
  console.log('\n📡 Sinal SIGTERM recebido. Encerrando...');
  await mongoose.connection.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('\n📡 Sinal SIGINT recebido. Encerrando...');
  await mongoose.connection.close();
  process.exit(0);
});
