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
    return res.status(200).json({ status: 'online', message: 'Servidor Unificado Prime Studio em execução' });
});

// Map para controle de conexões ativas do Socket (socket.id -> userId)
const connectedUsers = new Map();

// ================= TEMPO REAL (SOCKET.IO) =================
io.on('connection', (socket) => {

    // Conexão do Usuário
    socket.on('join_user_room', async (userId) => {
        if (userId && mongoose.Types.ObjectId.isValid(userId)) {
            const uIdStr = userId.toString();
            socket.join(uIdStr);
            connectedUsers.set(socket.id, uIdStr);
            console.log(`[Socket] Usuário conectado à sala privativa: ${uIdStr}`);

            try {
                await User.findByIdAndUpdate(uIdStr, { isOnline: true, lastSeen: new Date() });
                io.to('admin_support_room').emit('user_status_changed', { userId: uIdStr, isOnline: true, lastSeen: "" });
            } catch (e) {
                console.error('[Socket] Erro ao atualizar status online:', e);
            }
        }
    });

    // Conexão do Admin
    socket.on('join_admin_support', async (userId) => {
        if (userId) {
            if (mongoose.Types.ObjectId.isValid(userId)) {
                const uIdStr = userId.toString();
                connectedUsers.set(socket.id, uIdStr);
                try {
                    await User.findByIdAndUpdate(uIdStr, { isOnline: true, lastSeen: new Date() });
                } catch (e) {
                    console.error('[Socket] Erro ao atualizar status online do admin:', e);
                }
            }
            socket.join('admin_support_room');
            console.log(`[Socket] Admin conectado ao painel de suporte: ${userId}`);
        }
    });

    // Indicador de "Digitando..." unificado (Cliente -> Admin)
    socket.on('user_typing', (data) => {
        if (data && data.userId) {
            io.to('admin_support_room').emit('typing_status', { 
                userId: data.userId, 
                isTyping: data.isTyping 
            });
        }
    });

    // Indicador de "Digitando..." unificado (Admin -> Cliente)
    socket.on('support_typing', (data) => {
        if (data && data.userId) {
            io.to(data.userId.toString()).emit('support_typing', { 
                userId: data.userId, 
                isTyping: data.isTyping,
                name: data.name || "Suporte Cine GO!"
            });
        }
    });

    // Status Online do Atendente para o Cliente
    socket.on('support_status', (data) => {
        if (data) {
            io.emit('support_status', data);
        }
    });

    // Confirmação de Leitura de Mensagens (Selos Azul / Read)
    socket.on('mark_as_read', async (targetUserId) => {
        const uId = typeof targetUserId === 'string' ? targetUserId : (targetUserId && targetUserId.userId ? targetUserId.userId : null);
        if (!uId) return;

        try {
            const now = new Date();
            await SupportMessage.updateMany(
                { userId: uId, status: { $ne: 'read' } },
                { $set: { status: 'read', readAt: now } }
            );

            const payload = { userId: uId, status: 'read', readAt: now };
            io.to(uId.toString()).emit('messages_read', payload);
            io.to('admin_support_room').emit('messages_read', payload);
        } catch (err) {
            console.error('[Socket] Erro ao marcar como lido via Socket:', err);
        }
    });

    // Confirmação de Entrega de Mensagens
    socket.on('mark_as_delivered', async (data) => {
        const uId = typeof data === 'string' ? data : (data && data.userId ? data.userId : null);
        if (!uId) return;

        try {
            await SupportMessage.updateMany(
                { userId: uId, status: 'sent' },
                { $set: { status: 'delivered' } }
            );

            const payload = { userId: uId, status: 'delivered' };
            io.to(uId.toString()).emit('messages_delivered', payload);
            io.to('admin_support_room').emit('messages_delivered', payload);
        } catch (err) {
            console.error('[Socket] Erro ao marcar como entregue via Socket:', err);
        }
    });

    // Desconexão (Atualiza Visto por Último / Last Seen)
    socket.on('disconnect', async () => {
        const uId = connectedUsers.get(socket.id);
        if (uId) {
            connectedUsers.delete(socket.id);

            const activeSockets = Array.from(connectedUsers.values()).filter(id => id === uId);
            if (activeSockets.length === 0) {
                const lastSeen = new Date();
                try {
                    await User.findByIdAndUpdate(uId, { isOnline: false, lastSeen });
                    io.to('admin_support_room').emit('user_status_changed', { 
                        userId: uId, 
                        isOnline: false, 
                        lastSeen: lastSeen.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) 
                    });
                    console.log(`[Socket] Usuário offline: ${uId}`);
                } catch (e) {
                    console.error('[Socket] Erro ao atualizar desconexão:', e);
                }
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
        studio: user.studio !== undefined ? user.studio : false,
        recoveryCode: user.recoveryCode || "",
        profile: user.profile || {},
        isOnline: user.isOnline || false,
        lastSeen: user.lastSeen || null,
        createdAt: user.createdAt || null
    };
    io.to(userId.toString()).emit('user_updated', userDataPayload);
}

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

const UserSchema = new mongoose.Schema({
    email: { type: String, unique: true },
    password: String,
    name: String,
    avatar: String,
    plan: { type: String, default: 'FREE' },
    studio: { type: Boolean, default: false },
    purchaseToken: { type: String, default: null },
    recoveryCode: { type: String, unique: true },
    profile: { type: mongoose.Schema.Types.Mixed, default: {} },
    isOnline: { type: Boolean, default: false },
    lastSeen: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now }
});

const User = mongoose.model('User', UserSchema);

const SupportMessageSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    senderId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    senderModel: { type: String, enum: ['user', 'admin'], required: true },
    message: { type: String, required: true },
    status: { type: String, enum: ['sent', 'delivered', 'read'], default: 'sent' },
    readAt: { type: Date, default: null },
    createdAt: { type: Date, default: Date.now }
});

const SupportMessage = mongoose.model('SupportMessage', SupportMessageSchema);

mongoose.connect(process.env.MONGO_URL)
.then(async () => {
    console.log('MongoDB conectado com sucesso');
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

app.post('/upload-avatar', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'Arquivo não enviado' });
        if (STORAGE === "r2") {
            const ext = req.file.originalname.split('.').pop();
            const fileName = `avatars/${Date.now()}-${Math.random().toString(36).substring(2)}.${ext}`;
            await r2.send(
                new PutObjectCommand({
                    Bucket: process.env.R2_BUCKET,
                    Key: fileName,
                    Body: req.file.buffer,
                    ContentType: req.file.mimetype,
                    CacheControl: 'public, max-age=31536000'
                })
            );
            return res.json({ success: true, url: `${process.env.R2_PUBLIC_URL}/${fileName}` });
        }
        if (STORAGE === "cloudinary") {
            const result = await new Promise((resolve, reject) => {
                const stream = cloudinary.uploader.upload_stream(
                    { folder: "avatars" },
                    (error, result) => {
                        if (error) reject(error);
                        else resolve(result);
                    }
                );
                streamifier.createReadStream(req.file.buffer).pipe(stream);
            });
            return res.json({ success: true, url: result.secure_url });
        }
        return res.status(500).json({ error: "Storage não configurado" });
    } catch (err) {
        return res.status(500).json({ error: "Erro no upload" });
    }
});

app.post('/register', async (req, res) => {
    const { email, password, name, avatar, plan = 'FREE', profile = {} } = req.body;
    if (!email || !password || !name) return res.status(400).json({ error: 'Preencha todos os campos' });

    try {
        const exists = await User.findOne({ email });
        if (exists) return res.status(400).json({ error: 'Email já cadastrado' });

        const hash = await bcrypt.hash(password, 10);
        const recoveryCode = await createUniqueRecoveryCode();

        const user = await User.create({ email, password: hash, name, avatar, plan, studio: false, recoveryCode, profile });
        return res.json({
            success: true,
            recoveryCode,
            user: { id: user._id, name: user.name, email: user.email, avatar: user.avatar, plan: user.plan, studio: user.studio, recoveryCode: user.recoveryCode, profile: user.profile, isOnline: user.isOnline, lastSeen: user.lastSeen }
        });
    } catch (err) {
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

        const token = jwt.sign({ id: user._id }, process.env.JWT_SECRET, { expiresIn: '30d' });
        return res.json({
            token,
            user: { id: user._id, name: user.name, email: user.email, avatar: user.avatar, plan: user.plan || 'FREE', studio: user.studio !== undefined ? user.studio : false, recoveryCode: user.recoveryCode, profile: user.profile, isOnline: user.isOnline, lastSeen: user.lastSeen }
        });
    } catch (err) {
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.get('/me', auth, async (req, res) => {
    try {
        const user = await User.findById(req.userId).select('-password');
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });
        return res.json({
            user: { id: user._id, name: user.name, email: user.email, avatar: user.avatar, plan: user.plan || 'FREE', studio: user.studio !== undefined ? user.studio : false, recoveryCode: user.recoveryCode, profile: user.profile, isOnline: user.isOnline, lastSeen: user.lastSeen }
        });
    } catch (err) {
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/profile', auth, async (req, res) => {
    const { name, avatar, profile = {} } = req.body;
    try {
        const user = await User.findById(req.userId);
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        if (typeof name === 'string') user.name = name.trim();
        if (typeof avatar === 'string' && avatar.trim()) user.avatar = avatar;
        user.profile = { ...user.profile, ...profile };
        await user.save();

        return res.json({
            success: true,
            user: { id: user._id, name: user.name, email: user.email, avatar: user.avatar, plan: user.plan || 'FREE', recoveryCode: user.recoveryCode, profile: user.profile, isOnline: user.isOnline, lastSeen: user.lastSeen }
        });
    } catch (err) {
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.get('/support/messages/:targetUserId?', auth, async (req, res) => {
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser) return res.status(404).json({ error: 'Usuário não encontrado' });

        let queryUserId = req.userId;
        if (requestingUser.studio && req.params.targetUserId) {
            queryUserId = req.params.targetUserId;
        }

        const messages = await SupportMessage.find({ userId: queryUserId })
            .sort({ createdAt: 1 })
            .populate('senderId', 'name avatar email isOnline lastSeen');

        return res.json({ success: true, messages });
    } catch (err) {
        return res.status(500).json({ error: 'Erro ao buscar mensagens' });
    }
});

app.get('/support/admin/chats', auth, async (req, res) => {
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser || !requestingUser.studio) {
            return res.status(403).json({ error: 'Acesso negado.' });
        }

        const chats = await SupportMessage.aggregate([
            { $sort: { createdAt: -1 } },
            {
                $group: {
                    _id: "$userId",
                    lastMessage: { $first: "$message" },
                    lastMessageDate: { $first: "$createdAt" },
                    lastMessageStatus: { $first: "$status" },
                    lastMessageSenderModel: { $first: "$senderModel" },
                    unreadCount: {
                        $sum: {
                            $cond: [
                                { $and: [
                                    { $eq: ["$senderModel", "user"] },
                                    { $ne: ["$status", "read"] }
                                ]},
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            { $sort: { lastMessageDate: -1 } }
        ]);

        const populatedChats = await User.populate(chats, {
            path: '_id',
            select: 'name email avatar plan studio isOnline lastSeen'
        });

        return res.json({ success: true, chats: populatedChats });
    } catch (err) {
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

        if (sender.studio && targetUserId) {
            chatUserId = targetUserId;
            senderModel = 'admin';
        }

        const newMessage = await SupportMessage.create({
            userId: chatUserId,
            senderId: sender._id,
            senderModel: senderModel,
            message: message.trim(),
            status: 'sent',
            readAt: null
        });

        const populatedMessage = await SupportMessage.findById(newMessage._id)
            .populate('senderId', 'name avatar email isOnline lastSeen');

        io.to(chatUserId.toString()).emit('new_support_message', populatedMessage);
        io.to('admin_support_room').emit('new_support_message', populatedMessage);

        return res.json({ success: true, message: populatedMessage });
    } catch (err) {
        return res.status(500).json({ error: 'Erro ao enviar mensagem' });
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor unificado rodando na porta ${PORT}`);
});
