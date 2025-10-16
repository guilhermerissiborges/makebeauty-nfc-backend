const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const crypto = require('crypto');
require('dotenv').config();

const app = express();

// ========================================
// MIDDLEWARES
// ========================================
app.use(cors());
app.use(express.json());

// 🆕 Middleware de log detalhado
app.use((req, res, next) => {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${req.method} ${req.path}`);
  next();
});

// ========================================
// CONECTAR AO MONGODB
// ========================================
mongoose.connect(process.env.MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  console.log('✅ Conectado ao MongoDB!');
}).catch((err) => {
  console.error('❌ Erro ao conectar MongoDB:', err);
  process.exit(1); // 🆕 Encerra se MongoDB falhar
});

// ========================================
// SCHEMA DO PRODUTO
// ========================================
const ProductSchema = new mongoose.Schema({
  nfcUID: { type: String, required: true, unique: true, index: true }, // 🆕 index
  productId: { type: String, required: true, unique: true },
  productName: String,
  batchNumber: String,
  manufacturingDate: Date,
  manufacturingDateTime: Date, // 🆕 campo adicional
  expiryDate: Date,
  manufacturingLocation: String,
  secretKey: String,
  scanCount: { type: Number, default: 0 },
  isActive: { type: Boolean, default: true },
  syncedFromSheets: { type: Boolean, default: false },
  scans: [{
    timestamp: { type: Date, default: Date.now },
    location: String,
    ipAddress: String, // 🆕 rastrear IP
    userAgent: String  // 🆕 rastrear dispositivo
  }],
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now } // 🆕 campo de atualização
});

// 🆕 Middleware para atualizar updatedAt
ProductSchema.pre('save', function(next) {
  this.updatedAt = new Date();
  next();
});

const Product = mongoose.model('Product', ProductSchema);

// ========================================
// 🆕 FUNÇÕES AUXILIARES
// ========================================

/**
 * 🆕 Valida se o UID tem formato válido (hex, 14 ou 20 caracteres)
 */
function isValidUID(uid) {
  if (!uid) return false;
  const normalized = uid.replace(/[:\s-]/g, '').toUpperCase().trim();
  const hexPattern = /^[0-9A-F]{14,20}$/;
  return hexPattern.test(normalized);
}

/**
 * 🆕 Detecta padrão suspeito de clonagem
 */
function detectClonePattern(scans) {
  if (scans.length < 2) return { suspicious: false };
  
  // Verificar múltiplos scans em curto período (< 1 minuto)
  const recentScans = scans.filter(scan => {
    const timeDiff = Date.now() - new Date(scan.timestamp).getTime();
    return timeDiff < 60000; // 1 minuto
  });
  
  if (recentScans.length > 5) {
    return { suspicious: true, reason: 'Múltiplos scans em menos de 1 minuto' };
  }
  
  // Verificar IPs muito diferentes em 24h
  const last24h = scans.filter(scan => {
    const timeDiff = Date.now() - new Date(scan.timestamp).getTime();
    return timeDiff < 86400000; // 24 horas
  });
  
  const uniqueIPs = [...new Set(last24h.map(s => s.ipAddress).filter(Boolean))];
  if (uniqueIPs.length > 10) {
    return { suspicious: true, reason: 'Muitos IPs diferentes em 24h' };
  }
  
  return { suspicious: false };
}

// ========================================
// ENDPOINT: VERIFICAR PRODUTO
// ========================================
app.post('/api/verify-product', async (req, res) => {
  const startTime = Date.now(); // 🆕 medir tempo de resposta
  
  try {
    const { uid, signature, counter } = req.body;
    
    // 🆕 Validação de entrada
    if (!uid) {
      console.log('❌ Erro: UID não fornecido');
      return res.status(400).json({
        success: false,
        authentic: false,
        error: 'UID da tag NFC é obrigatório',
        details: 'Por favor, forneça o UID do produto.'
      });
    }
    
    // 🆕 Validar formato do UID
    if (!isValidUID(uid)) {
      console.log(`❌ Erro: UID inválido - ${uid}`);
      return res.status(400).json({
        success: false,
        authentic: false,
        error: 'Formato de UID inválido',
        details: 'O UID fornecido não está no formato correto.'
      });
    }

    // Normalizar UID - remover ":" e espaços
    const normalizedUID = uid.replace(/[:\s]/g, '').toUpperCase();
    console.log(`🔍 Buscando produto com UID: ${normalizedUID}`); // 🆕 log melhorado

    // Buscar produto
    const product = await Product.findOne({ nfcUID: normalizedUID });
    
    if (!product) {
      console.log(`❌ UID não encontrado no banco: ${normalizedUID}`); // 🆕 log
      return res.status(404).json({
        success: false,
        authentic: false,
        error: 'Tag NFC não registrada no sistema Make Beauty',
        details: 'Este produto não foi encontrado em nossa base de dados. Verifique se a tag foi registrada corretamente.'
      });
    }

    console.log(`✅ Produto encontrado: ${product.productName} (${product.productId})`); // 🆕 log

    if (!product.isActive) {
      console.log(`⚠️ Produto inativo: ${normalizedUID}`); // 🆕 log
      return res.status(403).json({
        success: false,
        authentic: false,
        error: 'Produto foi bloqueado ou recolhido',
        details: 'Este produto foi marcado como inativo em nosso sistema. Entre em contato com o suporte.'
      });
    }

    // Validar assinatura (pular validação para UIDs de DEMO e produtos da planilha)
    const isDemoUID = normalizedUID.includes('AABBCCDDDEEFF') || 
                      normalizedUID.includes('112233445566') ||
                      normalizedUID.includes('DEMO'); // 🆕 adicional
    const isFromSheets = product.syncedFromSheets === true;
    
    // 🆕 Log melhorado
    console.log(`🔐 Tipo de validação:`);
    console.log(`   - Demo UID: ${isDemoUID}`);
    console.log(`   - Sincronizado do Sheets: ${isFromSheets}`);
    
    if (!isDemoUID && !isFromSheets && signature && product.secretKey) {
      const expectedSignature = crypto
        .createHmac('sha256', product.secretKey)
        .update(`${uid}:${counter}`)
        .digest('hex');

      if (signature !== expectedSignature) {
        console.log(`❌ Assinatura inválida para UID: ${normalizedUID}`); // 🆕 log
        return res.status(403).json({
          success: false,
          authentic: false,
          error: 'Assinatura criptográfica inválida',
          details: 'A assinatura digital desta tag não confere. Produto possivelmente falsificado.'
        });
      }
      console.log(`✅ Assinatura validada com sucesso`); // 🆕 log
    } else {
      console.log(`ℹ️ Validação de assinatura pulada (produto do Sheets ou Demo)`); // 🆕 log
    }

    // Validar contador (pular validação para produtos da planilha e DEMO)
    if (!isDemoUID && !isFromSheets && counter) {
      if (counter <= product.scanCount) {
        console.log(`❌ Contador inválido para UID: ${normalizedUID}`); // 🆕 log
        return res.status(403).json({
          success: false,
          authentic: false,
          error: 'Contador de leituras inválido - tag possivelmente clonada',
          details: 'O contador de verificações não está correto.'
        });
      }
    }

    // 🆕 Coletar informações do scan
    const ipAddress = req.headers['x-forwarded-for'] || 
                     req.headers['x-real-ip'] || 
                     req.connection.remoteAddress || 
                     req.socket.remoteAddress ||
                     'Unknown';
    
    const userAgent = req.headers['user-agent'] || 'Unknown';
    const location = req.body.location || 'Web';

    // Atualizar produto
    product.scanCount = counter || (product.scanCount + 1); // 🆕 incrementa se counter não fornecido
    product.scans.push({
      timestamp: new Date(),
      location: location,
      ipAddress: ipAddress, // 🆕
      userAgent: userAgent   // 🆕
    });

    // 🆕 Detectar padrão de clonagem
    const cloneCheck = detectClonePattern(product.scans);
    if (cloneCheck.suspicious) {
      console.log(`⚠️ ALERTA: Padrão suspeito detectado - ${cloneCheck.reason}`);
    }

    await product.save();

    // Retornar sucesso
    const ageInDays = product.manufacturingDate ? 
      Math.floor((Date.now() - product.manufacturingDate.getTime()) / (1000 * 60 * 60 * 24)) : 
      null;

    const isExpired = product.expiryDate ? 
      Date.now() > product.expiryDate.getTime() : 
      false;

    const status = isExpired ? 'Vencido' : 'Válido';

    // 🆕 Tempo de resposta
    const responseTime = Date.now() - startTime;

    const response = {
      success: true,
      authentic: true,
      product: {
        productId: product.productId,
        name: product.productName,
        batchNumber: product.batchNumber,
        manufacturingDate: product.manufacturingDate,
        expiryDate: product.expiryDate,
        manufacturingLocation: product.manufacturingLocation,
        ageInDays: ageInDays,
        scanCount: product.scanCount,
        isFirstScan: product.scanCount === 1,
        status: status,
        isExpired: isExpired // 🆕
      },
      verification: { // 🆕 informações adicionais
        timestamp: new Date().toISOString(),
        responseTimeMs: responseTime,
        suspicious: cloneCheck.suspicious || false
      }
    };

    // 🆕 Log de sucesso detalhado
    console.log(`✅ VALIDAÇÃO CONCLUÍDA: ${product.productName}`);
    console.log(`   Scan #${product.scanCount} | Status: ${status} | Tempo: ${responseTime}ms`);

    if (cloneCheck.suspicious) {
      response.warning = cloneCheck.reason;
    }

    res.json(response);

  } catch (error) {
    console.error('❌ ERRO INTERNO:', error);
    res.status(500).json({
      success: false,
      authentic: false,
      error: 'Erro interno do servidor',
      details: 'Ocorreu um erro ao processar sua solicitação. Tente novamente em instantes.'
    });
  }
});

// ENDPOINT: Registrar novo produto (Admin)
app.post('/api/admin/register-product', async (req, res) => {
  try {
    const { adminKey, nfcUID, productData } = req.body;

    // Validar chave admin
    if (!adminKey || adminKey !== process.env.ADMIN_SECRET_KEY) {
      console.log('❌ Tentativa de acesso admin com chave inválida'); // 🆕 log
      return res.status(403).json({
        success: false,
        error: 'Chave de administrador inválida'
      });
    }

    // 🆕 Normalizar e validar UID
    const normalizedUID = nfcUID.replace(/[:\s]/g, '').toUpperCase();
    
    if (!isValidUID(normalizedUID)) {
      return res.status(400).json({
        success: false,
        error: 'Formato de UID inválido'
      });
    }

    // Verificar se já existe
    const exists = await Product.findOne({ nfcUID: normalizedUID });
    if (exists) {
      return res.status(409).json({
        success: false,
        error: 'Tag NFC já registrada no sistema'
      });
    }

    // Gerar chave secreta
    const secretKey = crypto.randomBytes(32).toString('hex');

    // Criar produto
    const newProduct = new Product({
      nfcUID: normalizedUID, // 🆕 usar UID normalizado
      ...productData,
      secretKey: crypto.createHash('sha256').update(secretKey).digest('hex'),
      syncedFromSheets: false // 🆕 explícito
    });

    await newProduct.save();

    console.log(`✅ Novo produto registrado: ${newProduct.productId}`); // 🆕 log

    res.json({
      success: true,
      message: 'Produto registrado com sucesso!',
      productId: newProduct.productId,
      secretKey: secretKey // 🆕 retornar apenas na criação
    });

  } catch (error) {
    console.error('❌ Erro ao registrar produto:', error);
    res.status(500).json({
      success: false,
      error: 'Erro ao registrar produto'
    });
  }
});

// ENDPOINT DE DEBUG - Ver dados do produto
app.get('/api/debug/product/:uid', async (req, res) => {
  try {
    const normalizedUID = req.params.uid.replace(/[:\s]/g, '').toUpperCase();
    const product = await Product.findOne({ nfcUID: normalizedUID });
    
    if (!product) {
      return res.json({ 
        found: false, 
        searchedUID: normalizedUID, // 🆕 mostrar UID buscado
        message: 'Produto não encontrado no banco de dados'
      });
    }
    
    // Retornar TODOS os campos (exceto secretKey completa)
    res.json({
      found: true,
      uid: product.nfcUID,
      productId: product.productId,
      productName: product.productName,
      batchNumber: product.batchNumber,
      manufacturingDate: product.manufacturingDate,
      manufacturingDateTime: product.manufacturingDateTime,
      expiryDate: product.expiryDate,
      manufacturingLocation: product.manufacturingLocation,
      syncedFromSheets: product.syncedFromSheets,
      hasSecretKey: !!product.secretKey,
      scanCount: product.scanCount,
      totalScans: product.scans?.length || 0,
      lastScan: product.scans?.length > 0 ? product.scans[product.scans.length - 1] : null, // 🆕 último scan
      isActive: product.isActive,
      createdAt: product.createdAt,
      updatedAt: product.updatedAt // 🆕
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 🆕 ENDPOINT: Health Check
app.get('/health', async (req, res) => {
  try {
    // Verificar conexão MongoDB
    const dbState = mongoose.connection.readyState;
    const dbStatus = {
      0: 'Disconnected',
      1: 'Connected',
      2: 'Connecting',
      3: 'Disconnecting'
    };
    
    // Contar produtos
    const totalProducts = await Product.countDocuments();
    const activeProducts = await Product.countDocuments({ isActive: true });
    
    res.json({
      status: 'online',
      timestamp: new Date().toISOString(),
      database: {
        status: dbStatus[dbState],
        connected: dbState === 1,
        totalProducts,
        activeProducts
      },
      uptime: process.uptime(),
      version: '2.0.0'
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message
    });
  }
});

// Página inicial (teste)
app.get('/', (req, res) => {
  res.json({
    message: '🔐 API TrueTouch™ Make Beauty',
    status: 'online',
    version: '2.0.0', // 🆕
    endpoints: {
      verify: 'POST /api/verify-product',
      register: 'POST /api/admin/register-product',
      debug: 'GET /api/debug/product/:uid', // 🆕
      health: 'GET /health' // 🆕
    }
  });
});

// 🆕 Tratamento de rotas não encontradas
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint não encontrado',
    availableEndpoints: [
      'POST /api/verify-product',
      'POST /api/admin/register-product',
      'GET /api/debug/product/:uid',
      'GET /health'
    ]
  });
});

// 🆕 Tratamento de erros global
app.use((err, req, res, next) => {
  console.error('❌ ERRO NÃO TRATADO:', err);
  res.status(500).json({
    success: false,
    error: 'Erro interno do servidor',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// Iniciar servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('\n' + '='.repeat(50));
  console.log('🚀 SERVIDOR TRUETOUCH™ V2.0 INICIADO');
  console.log('='.repeat(50));
  console.log(`📡 Porta: ${PORT}`);
  console.log(`🌐 URL: http://localhost:${PORT}`);
  console.log(`🗄️  MongoDB: ${mongoose.connection.readyState === 1 ? '✅ Conectado' : '⏳ Conectando...'}`);
  console.log(`🔐 Admin Key: ${process.env.ADMIN_SECRET_KEY ? '✅ Configurada' : '❌ NÃO CONFIGURADA'}`);
  console.log('='.repeat(50) + '\n');
});

// 🆕 Tratamento de sinais de encerramento
process.on('SIGTERM', async () => {
  console.log('📡 Sinal SIGTERM recebido. Encerrando gracefully...');
  await mongoose.connection.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('\n📡 Sinal SIGINT recebido. Encerrando gracefully...');
  await mongoose.connection.close();
  process.exit(0);
});
