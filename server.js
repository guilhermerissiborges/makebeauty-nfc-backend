const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const crypto = require('crypto');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Conectar ao MongoDB
mongoose.connect(process.env.MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => {
  console.log('✅ Conectado ao MongoDB!');
}).catch((err) => {
  console.error('❌ Erro ao conectar MongoDB:', err);
});

// Schema do Produto
const ProductSchema = new mongoose.Schema({
  nfcUID: { type: String, required: true, unique: true },
  productId: { type: String, required: true, unique: true },
  productName: String,
  batchNumber: String,
  manufacturingDate: Date,
  expiryDate: Date,
  manufacturingLocation: String,
  secretKey: String,
  scanCount: { type: Number, default: 0 },
  isActive: { type: Boolean, default: true },
  scans: [{
    timestamp: Date,
    location: String
  }],
  createdAt: { type: Date, default: Date.now }
});

const Product = mongoose.model('Product', ProductSchema);

// ENDPOINT: Verificar produto
app.post('/api/verify-product', async (req, res) => {
  try {
    const { uid, signature, counter } = req.body;


    // Buscar produto
    const product = await Product.findOne({ nfcUID: normalizedUID });
    // Normalizar UID - remover ":" e espaços
const normalizedUID = uid.replace(/[:\s]/g, '').toUpperCase();
    if (!product) {
      return res.status(404).json({
        success: false,
        authentic: false,
        error: 'Tag NFC não registrada no sistema Make Beauty'
      });
    }

    if (!product.isActive) {
      return res.status(403).json({
        success: false,
        authentic: false,
        error: 'Produto foi bloqueado ou recolhido'
      });
    }

   // Validar assinatura (pular validação para UIDs de DEMO e produtos da planilha)
const isDemoUID = normalizedUID.includes('AABBCCDDDEEFF') || normalizedUID.includes('112233445566');
const isFromSheets = product.syncedFromSheets === true;

if (!isDemoUID && !isFromSheets) {
  const expectedSignature = crypto
    .createHmac('sha256', product.secretKey)
    .update(`${uid}:${counter}`)
    .digest('hex');

  if (signature !== expectedSignature) {
    return res.status(403).json({
      success: false,
      authentic: false,
      error: 'Assinatura inválida - produto possivelmente falsificado'
    });
  }
}

    // Validar contador
    if (counter <= product.scanCount) {
      return res.status(403).json({
        success: false,
        authentic: false,
        error: 'Contador de leituras inválido - tag possivelmente clonada'
      });
    }

    // Atualizar produto
    product.scanCount = counter;
    product.scans.push({
      timestamp: new Date(),
      location: req.body.location || 'Web'
    });
    await product.save();

    // Retornar sucesso
    const ageInDays = Math.floor(
      (Date.now() - product.manufacturingDate.getTime()) / (1000 * 60 * 60 * 24)
    );

    res.json({
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
        status: Date.now() < product.expiryDate.getTime() ? 'Válido' : 'Vencido'
      }
    });

  } catch (error) {
    console.error('Erro:', error);
    res.status(500).json({
      success: false,
      error: 'Erro interno do servidor'
    });
  }
});

// ENDPOINT: Registrar novo produto (Admin)
app.post('/api/admin/register-product', async (req, res) => {
  try {
    const { adminKey, nfcUID, productData } = req.body;

    // Validar chave admin
    if (adminKey !== process.env.ADMIN_SECRET_KEY) {
      return res.status(403).json({
        success: false,
        error: 'Chave de administrador inválida'
      });
    }

    // Verificar se já existe
    const exists = await Product.findOne({ nfcUID });
    if (exists) {
      return res.status(409).json({
        success: false,
        error: 'Tag NFC já registrada'
      });
    }

    // Gerar chave secreta
    const secretKey = crypto.randomBytes(32).toString('hex');

    // Criar produto
    const newProduct = new Product({
      nfcUID,
      ...productData,
      secretKey: crypto.createHash('sha256').update(secretKey).digest('hex')
    });

    await newProduct.save();

    res.json({
      success: true,
      message: 'Produto registrado com sucesso!',
      productId: newProduct.productId,
      secretKeyToProgramInTag: secretKey
    });

  } catch (error) {
    console.error('Erro:', error);
    res.status(500).json({
      success: false,
      error: 'Erro ao registrar produto'
    });
  }
});

// Página inicial (teste)
app.get('/', (req, res) => {
  res.json({
    message: '🔐 API TrueTouch™ Make Beauty',
    status: 'online',
    endpoints: {
      verify: 'POST /api/verify-product',
      register: 'POST /api/admin/register-product'
    }
  });
});
// ENDPOINT DE DEBUG - Ver dados do produto
app.get('/api/debug/product/:uid', async (req, res) => {
  try {
    const uid = req.params.uid;
    const product = await Product.findOne({ nfcUID: uid });
    
    if (!product) {
      return res.json({ found: false, uid });
    }
    
    res.json({
      found: true,
      uid: product.nfcUID,
      productId: product.productId,
      syncedFromSheets: product.syncedFromSheets,
      hasSecretKey: !!product.secretKey,
      manufacturingDateTime: product.manufacturingDateTime,
      isActive: product.isActive
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
// Iniciar servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Servidor rodando na porta ${PORT}`);
  console.log(`📡 Acesse: http://localhost:${PORT}`);
});