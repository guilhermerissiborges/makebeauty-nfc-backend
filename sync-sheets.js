// ========================================
// SINCRONIZAÇÃO GOOGLE SHEETS → MONGODB
// ========================================
// Instalar dependências:
// npm install googleapis mongoose dotenv

const { google } = require('googleapis');
const mongoose = require('mongoose');
const crypto = require('crypto');
require('dotenv').config();

// Conectar ao MongoDB
mongoose.connect(process.env.MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  console.log('✅ Conectado ao MongoDB');
}).catch((err) => {
  console.error('❌ Erro ao conectar MongoDB:', err);
  process.exit(1);
});

// Schema do Produto
const ProductSchema = new mongoose.Schema({
  nfcUID: { type: String, required: true, unique: true },
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
  syncedFromSheets: { type: Boolean, default: true }
});

const Product = mongoose.model('Product', ProductSchema);

// ========================================
// CONFIGURAR GOOGLE SHEETS API
// ========================================
async function getGoogleSheetsClient() {
  const auth = new google.auth.GoogleAuth({
    keyFile: './credentials.json', // Arquivo de credenciais
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const client = await auth.getClient();
  const sheets = google.sheets({ version: 'v4', auth: client });
  
  return sheets;
}

// ========================================
// LER DADOS DO GOOGLE SHEETS
// ========================================
async function readGoogleSheet(spreadsheetId, range = 'Sheet1!A:B') {
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
    const data = [];

    // Converter linhas em objetos
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const obj = {};
      
      headers.forEach((header, index) => {
        obj[header] = row[index];
      });
      
      data.push(obj);
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
    
    // Ler dados da planilha
    // Ajustar range se sua planilha tiver nome diferente ou mais colunas
    const data = await readGoogleSheet(spreadsheetId, 'Sheet1!A:B');
    
    console.log(`📋 Encontradas ${data.length} linhas\n`);

    let novos = 0;
    let atualizados = 0;
    let erros = 0;

    for (const row of data) {
      try {
        const nfcUID = row['UID'];
        const dataHora = row['Data_Hora'];
        
        if (!nfcUID || !dataHora) {
          console.log('⚠️  Linha sem UID ou Data_Hora, pulando...');
          continue;
        }

        // Converter data/hora
        let manufacturingDateTime;
        
        // Tentar vários formatos comuns
        if (dataHora.includes('/')) {
          // Formato: dd/mm/yyyy HH:mm:ss
          const [datePart, timePart] = dataHora.split(' ');
          const [day, month, year] = datePart.split('/');
          const [hour, minute, second] = (timePart || '00:00:00').split(':');
          
          manufacturingDateTime = new Date(
            parseInt(year),
            parseInt(month) - 1,
            parseInt(day),
            parseInt(hour || 0),
            parseInt(minute || 0),
            parseInt(second || 0)
          );
        } else {
          // Tentar parse direto
          manufacturingDateTime = new Date(dataHora);
        }

        // Validar data
        if (isNaN(manufacturingDateTime.getTime())) {
          console.log(`⚠️  Data inválida para UID ${nfcUID}: ${dataHora}`);
          erros++;
          continue;
        }

        // Gerar lote baseado na data
        const lote = 'LT' + manufacturingDateTime.toISOString().slice(0,10).replace(/-/g,'');

        // ID do produto
        const productId = `MB-${lote}-${nfcUID.replace(/:/g, '').toUpperCase()}`;

        // Validade (2 anos)
        const expiryDate = new Date(manufacturingDateTime);
        expiryDate.setFullYear(expiryDate.getFullYear() + 2);

        // Chave secreta
        const secretKey = crypto.randomBytes(32).toString('hex');
        const secretKeyHash = crypto.createHash('sha256').update(secretKey).digest('hex');

        // Verificar se existe
        const existingProduct = await Product.findOne({ nfcUID });

        if (existingProduct) {
          // Atualizar
          existingProduct.manufacturingDateTime = manufacturingDateTime;
          existingProduct.manufacturingDate = new Date(manufacturingDateTime.toDateString());
          existingProduct.batchNumber = lote;
          await existingProduct.save();
          
          console.log(`🔄 Atualizado: ${nfcUID}`);
          atualizados++;
        } else {
          // Criar novo
          const newProduct = new Product({
            nfcUID,
            productId,
            productName: 'Produto Make Beauty',
            batchNumber: lote,
            manufacturingDate: new Date(manufacturingDateTime.toDateString()),
            manufacturingDateTime,
            expiryDate,
            manufacturingLocation: 'São Paulo, Brasil',
            secretKey: secretKeyHash,
            isActive: true,
            syncedFromSheets: true
          });

          await newProduct.save();
          
          console.log(`✅ Novo: ${nfcUID}`);
          console.log(`   ID: ${productId}`);
          console.log(`   Data/Hora: ${manufacturingDateTime.toLocaleString('pt-BR')}`);
          console.log(`   SecretKey: ${secretKey}\n`);
          
          novos++;
        }

      } catch (err) {
        console.error(`❌ Erro ao processar linha:`, err.message);
        erros++;
      }
    }

    // Resumo
    console.log('\n' + '='.repeat(50));
    console.log('📊 RESUMO DA SINCRONIZAÇÃO:');
    console.log('='.repeat(50));
    console.log(`✅ Novos: ${novos}`);
    console.log(`🔄 Atualizados: ${atualizados}`);
    console.log(`❌ Erros: ${erros}`);
    console.log(`📋 Total: ${data.length}`);
    console.log('='.repeat(50) + '\n');

  } catch (error) {
    console.error('❌ Erro na sincronização:', error);
    throw error;
  }
}

// ========================================
// EXECUTAR
// ========================================

// ID da planilha (pegar da URL do Google Sheets)
// URL: https://docs.google.com/spreadsheets/d/ID_AQUI/edit
const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_ID || process.argv[2];

if (!SPREADSHEET_ID) {
  console.error('❌ Erro: Forneça o ID da planilha');
  console.log('Uso: node sync-sheets.js ID_DA_PLANILHA');
  console.log('Ou configure GOOGLE_SHEETS_ID no .env');
  process.exit(1);
}

console.log('\n🚀 Iniciando sincronização...');
console.log(`📊 Planilha ID: ${SPREADSHEET_ID}\n`);

syncSheetsToMongoDB(SPREADSHEET_ID)
  .then(() => {
    console.log('✅ Sincronização concluída!');
    process.exit(0);
  })
  .catch((error) => {
    console.error('❌ Falha:', error);
    process.exit(1);
  });

// ========================================
// SINCRONIZAÇÃO AUTOMÁTICA
// ========================================

const INTERVALO_MINUTOS = 60;

console.log(`Modo automatico ativado (a cada ${INTERVALO_MINUTOS} min)`);

setInterval(async () => {
  console.log('\nExecutando sincronizacao agendada...');
  try {
    await syncSheetsToMongoDB(SPREADSHEET_ID);
  } catch (error) {
    console.error('Erro na sincronizacao agendada:', error);
  }
}, INTERVALO_MINUTOS * 60 * 1000);