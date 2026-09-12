const express = require('express');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const multer = require('multer');
const http = require('http');
const { Server } = require('socket.io');

const {
    S3Client,
    PutObjectCommand
} = require('@aws-sdk/client-s3');

const cloudinary = require('cloudinary').v2;
const streamifier = require('streamifier');

require('dotenv').config();

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(cors());
app.use(express.json());

// ================= ROTA PÚBLICA DE STATUS =================
app.get('/', (req, res) => {
    return res.status(200).json({ status: 'online', message: 'Servidor Prime Studio em execução' });
});

// ================= TEMPO REAL (SOCKET.IO) =================
io.on('connection', (socket) => {
    socket.on('join_user_room', (userId) => {
        if (userId) {
            socket.join(userId.toString());
            console.log(`[Socket] Usuário conectado à sala privativa: ${userId}`);
        }
    });

    socket.on('join_admin_support', (userId) => {
        if (userId) {
            socket.join('admin_support_room');
            console.log(`[Socket] Admin conectado ao painel de suporte: ${userId}`);
        }
    });

    socket.on('request_user_sync', async (userId) => {
        if (userId && mongoose.Types.ObjectId.isValid(userId)) {
            try {
                const user = await User.findById(userId).select('-password');
                if (user) {
                    notifyUserUpdate(user._id, user);
                }
            } catch (err) {
                console.error('[Socket] Erro ao sincronizar usuário:', err);
            }
        }
    });
});

function notifyUserUpdate(userId, user) {
    if (!userId || !user) return;

    const userDataPayload = {
        id: user._id ? user._id.toString() : user.id,
        name: user.name || "",
        email: user.email || "",
        avatar: user.avatar || "",
        plan: user.plan || 'FREE',
        acesso_liberado: user.acesso_liberado !== undefined ? user.acesso_liberado : false,
        isAdmin: user.isAdmin !== undefined ? user.isAdmin : false,
        recoveryCode: user.recoveryCode || "",
        profile: user.profile || {},
        createdAt: user.createdAt || null
    };

    io.to(userId.toString()).emit('user_updated', userDataPayload);
}

// ================= CHAVE MESTRA DA API =================
const API_KEY_SECRET = process.env.APP_API_KEY || "SUA_CHAVE_MESTRA_STREAMING_2026";

function verifyApiKey(req, res, next) {
    const clientApiKey = req.headers['x-api-key'];
    if (!clientApiKey || clientApiKey !== API_KEY_SECRET) {
        return res.status(403).json({ error: 'Acesso negado: Chave de API inválida ou ausente.' });
    }
    next();
}

app.use(verifyApiKey);

const STORAGE = process.env.STORAGE || "r2";

// ================= MONGO SCHEMAS =================
const UserSchema = new mongoose.Schema({
    email: { type: String, unique: true },
    password: String,
    name: String,
    avatar: String,
    plan: { type: String, default: 'FREE' },
    acesso_liberado: { type: Boolean, default: false },
    isAdmin: { type: Boolean, default: false },
    purchaseToken: { type: String, default: null },
    recoveryCode: { type: String, unique: true },
    profile: { type: mongoose.Schema.Types.Mixed, default: {} },
    createdAt: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);

const SupportMessageSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    senderId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    senderModel: { type: String, enum: ['user', 'admin'], required: true },
    message: { type: String, required: true },
    createdAt: { type: Date, default: Date.now }
});

const SupportMessage = mongoose.model('SupportMessage', SupportMessageSchema);

// Conexão com Auto-Injeção de Dados de Teste caso o banco esteja vazio
mongoose.connect(process.env.MONGO_URL)
.then(async () => {
    console.log('MongoDB conectado com sucesso');
    try {
        const totalMensagens = await SupportMessage.countDocuments();
        if (totalMensagens === 0) {
            const testUserId = new mongoose.Types.ObjectId("650f1a2b3c4d5e6f7a8b9c01");
            let userExistente = await User.findById(testUserId);
            if (!userExistente) {
                await User.create({
                    _id: testUserId,
                    email: "cliente.teste@email.com",
                    password: "$2a$10$fictitioushashforclienttest",
                    name: "João Teste (Cliente)",
                    plan: "PRO",
                    acesso_liberado: true,
                    isAdmin: false,
                    recoveryCode: "SG-1111-2222"
                });
            }
            await SupportMessage.create({
                userId: testUserId,
                senderId: testUserId,
                senderModel: "user",
                message: "Olá! Esta é uma mensagem de teste automática para o chat funcionar!"
            });
            console.log('>>> MENSAGEM E USUÁRIO DE TESTE CRIADOS COM SUCESSO NO BANCO! <<<');
        }
    } catch (e) {
        console.error('Erro ao criar dados de teste:', e);
    }
})
.catch(err => console.error('Erro ao conectar no MongoDB:', err));

function generateRecoveryCode() {
    const a = Math.floor(1000 + Math.random() * 9000);
    const b = Math.floor(1000 + Math.random() * 9000);
    return `SG-${a}-${b}`;
}

async function createUniqueRecoveryCode() {
    let code;
    do {
        code = generateRecoveryCode();
    } while (await User.findOne({ recoveryCode: code }));
    return code;
}

function auth(req, res, next) {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: 'Token de sessão ausente' });

    try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        req.userId = decoded.id;
        next();
    } catch {
        return res.status(401).json({ error: 'Token de sessão inválido ou expirado' });
    }
}

const r2 = new S3Client({
    region: 'auto',
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY
    }
});

cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
});

const upload = multer({ storage: multer.memoryStorage() });

// ================= ROTAS DE AUTENTICAÇÃO E PERFIL =================
app.post('/register', async (req, res) => {
    const { email, password, name, avatar, plan = 'FREE', acesso_liberado = false, isAdmin = false, profile = {} } = req.body;
    if (!email || !password || !name) return res.status(400).json({ error: 'Preencha todos os campos' });

    try {
        const exists = await User.findOne({ email });
        if (exists) return res.status(400).json({ error: 'Email já cadastrado' });

        const hash = await bcrypt.hash(password, 10);
        const recoveryCode = await createUniqueRecoveryCode();

        const user = await User.create({ email, password: hash, name, avatar, plan, acesso_liberado, isAdmin, recoveryCode, profile });

        return res.json({
            success: true,
            recoveryCode,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan,
                acesso_liberado: user.acesso_liberado,
                isAdmin: user.isAdmin,
                recoveryCode: user.recoveryCode,
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.post('/login', async (req, res) => {
    const { email, password } = req.body;
    try {
        const user = await User.findOne({ email });
        if (!user) return res.status(400).json({ error: 'Usuário não encontrado' });

        const ok = await bcrypt.compare(password, user.password);
        if (!ok) return res.status(401).json({ error: 'Senha inválida' });

        if (!user.acesso_liberado) {
            return res.status(403).json({ error: 'Acesso não liberado pelo Administrador. Entre em contato para ativar sua conta.' });
        }

        const token = jwt.sign({ id: user._id }, process.env.JWT_SECRET, { expiresIn: '30d' });

        return res.json({
            token,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                acesso_liberado: user.acesso_liberado,
                isAdmin: user.isAdmin,
                recoveryCode: user.recoveryCode,
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.get('/me', auth, async (req, res) => {
    try {
        const user = await User.findById(req.userId).select('-password');
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        return res.json({
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                acesso_liberado: user.acesso_liberado,
                isAdmin: user.isAdmin,
                recoveryCode: user.recoveryCode,
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

// ================= ROTAS DE SUPORTE (CHAT) =================
app.get('/support/messages/:targetUserId?', auth, async (req, res) => {
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser) return res.status(404).json({ error: 'Usuário não encontrado' });

        let queryUserId = req.userId;
        if (requestingUser.isAdmin && req.params.targetUserId) {
            queryUserId = req.params.targetUserId;
        }

        const messages = await SupportMessage.find({ userId: queryUserId })
            .sort({ createdAt: 1 })
            .populate('senderId', 'name avatar email');

        return res.json({ success: true, messages });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao buscar mensagens' });
    }
});

app.get('/support/admin/chats', auth, async (req, res) => {
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser || !requestingUser.isAdmin) {
            return res.status(403).json({ error: 'Acesso negado. Apenas administradores.' });
        }

        const chats = await SupportMessage.aggregate([
            { $sort: { createdAt: -1 } },
            {
                $group: {
                    _id: "$userId",
                    lastMessage: { $first: "$message" },
                    lastMessageDate: { $first: "$createdAt" }
                }
            },
            { $sort: { lastMessageDate: -1 } }
        ]);

        const populatedChats = await User.populate(chats, {
            path: '_id',
            select: 'name email avatar plan acesso_liberado'
        });

        return res.json({ success: true, chats: populatedChats });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao listar chats' });
    }
});

app.post('/support/message', auth, async (req, res) => {
    const { message, targetUserId } = req.body;
    if (!message || !message.trim()) return res.status(400).json({ error: 'A mensagem não pode estar vazia' });

    try {
        const sender = await User.findById(req.userId);
        if (!sender) return res.status(404).json({ error: 'Usuário não encontrado' });

        let chatUserId = sender._id;
        let senderModel = 'user';

        if (sender.isAdmin && targetUserId) {
            chatUserId = targetUserId;
            senderModel = 'admin';
        }

        const newMessage = await SupportMessage.create({
            userId: chatUserId,
            senderId: sender._id,
            senderModel: senderModel,
            message: message.trim()
        });

        const populatedMessage = await SupportMessage.findById(newMessage._id)
            .populate('senderId', 'name avatar email');

        io.to(chatUserId.toString()).emit('new_support_message', populatedMessage);
        io.to('admin_support_room').emit('new_support_message', populatedMessage);

        return res.json({ success: true, message: populatedMessage });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao enviar mensagem' });
    }
});

// ================= START =================
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor Prime Studio rodando na porta ${PORT}`);
});
