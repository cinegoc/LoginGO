const express = require('express');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const multer = require('multer');
const http = require('http');
const { Server } = require('socket.io');

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const cloudinary = require('cloudinary').v2;
const streamifier = require('streamifier');

require('dotenv').config();

// ================= CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE =================
const PORT = process.env.PORT || 3000;
const MONGO_URL = process.env.MONGO_URL || process.env.MONGODB_URI;
const JWT_SECRET = process.env.JWT_SECRET || "CHAVE_SECRETA_CINEGO_2026";
const API_KEY_SECRET = process.env.APP_API_KEY || "SUA_CHAVE_MESTRA_STREAMING_2026";
const STORAGE = process.env.STORAGE || "r2";

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(cors());
app.use(express.json());

// Rota de Health Check pública
app.get('/', (req, res) => {
    return res.status(200).json({ status: 'online', message: 'Servidor Unificado Prime Studio em execução' });
});

// Mapas de controle de conexões Socket para usuários e agentes (studio: true)
const userActiveSockets = new Map();
const agentActiveSockets = new Map();

async function setOfflineUser(userId, ioInstance, isAgent = false) {
    if (!userId || !mongoose.Types.ObjectId.isValid(userId)) return;
    const uIdStr = userId.toString();

    if (isAgent) {
        agentActiveSockets.delete(uIdStr);
    } else {
        userActiveSockets.delete(uIdStr);
    }

    const lastSeen = new Date();
    try {
        const user = await User.findById(uIdStr);
        if (!user) return;

        if (user.studio) {
            const totalAgentSockets = Array.from(agentActiveSockets.values()).reduce((acc, set) => acc + set.size, 0);
            if (totalAgentSockets > 0) return; 
        }

        await User.findByIdAndUpdate(uIdStr, { isOnline: false, lastSeen });
        const formattedLastSeen = lastSeen.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        if (user.studio) {
            ioInstance.emit('support_status', { 
                userId: uIdStr, 
                isOnline: false, 
                lastSeen: formattedLastSeen,
                agentName: "",
                agentAvatar: ""
            });
        } else {
            ioInstance.to('admin_support_room').emit('user_status_changed', { userId: uIdStr, isOnline: false, lastSeen: formattedLastSeen });
            ioInstance.to(uIdStr).emit('user_status_changed', { userId: uIdStr, isOnline: false, lastSeen: formattedLastSeen });
        }
    } catch (e) {
        console.error('[Socket] Erro crítico ao persistir status offline:', e);
    }
}

// ================= TEMPO REAL (SOCKET.IO) =================
io.on('connection', (socket) => {

    socket.on('join_user_room', async (userId) => {
        if (userId && mongoose.Types.ObjectId.isValid(userId)) {
            const uIdStr = userId.toString();
            socket.join(`user_${uIdStr}`);
            socket.join(uIdStr);

            if (!userActiveSockets.has(uIdStr)) {
                userActiveSockets.set(uIdStr, new Set());
            }
            userActiveSockets.get(uIdStr).add(socket.id);

            try {
                const user = await User.findById(uIdStr);
                if (user && !user.studio) {
                    await User.findByIdAndUpdate(uIdStr, { isOnline: true });
                    io.to('admin_support_room').emit('user_status_changed', { userId: uIdStr, isOnline: true, lastSeen: "" });
                    io.to(`user_${uIdStr}`).emit('user_status_changed', { userId: uIdStr, isOnline: true, lastSeen: "" });
                }
            } catch (e) {
                console.error('[Socket] Erro ao atualizar status online do usuário:', e);
            }
        }
    });

    socket.on('join_admin_support', async (adminId) => {
        const targetId = adminId || (socket.handshake.auth && socket.handshake.auth.userId);
        socket.join('admin_support_room');

        if (targetId && mongoose.Types.ObjectId.isValid(targetId)) {
            const uIdStr = targetId.toString();
            socket.join(`user_${uIdStr}`);
            socket.join(uIdStr);

            if (!agentActiveSockets.has(uIdStr)) {
                agentActiveSockets.set(uIdStr, new Set());
            }
            agentActiveSockets.get(uIdStr).add(socket.id);

            try {
                const adminUser = await User.findById(uIdStr);
                if (adminUser && adminUser.studio) {
                    await User.findByIdAndUpdate(uIdStr, { isOnline: true });
                    io.emit('support_status', { 
                        userId: uIdStr, 
                        isOnline: true, 
                        lastSeen: "",
                        agentName: adminUser.name,
                        agentAvatar: adminUser.avatar
                    });
                }
            } catch (e) {
                console.error('[Socket] Erro ao atualizar status online do admin:', e);
            }
        }
    });

    socket.on('support_status', (data) => {
        if (data) {
            io.emit('support_status', data);
        }
    });

    socket.on('user_typing', (data) => {
        if (data && data.userId) {
            const payload = { 
                userId: data.userId.toString(), 
                isTyping: Boolean(data.isTyping) 
            };
            io.to('admin_support_room').emit('user_typing', payload);
            io.to('admin_support_room').emit('typing_status', payload);
        }
    });

    socket.on('support_typing', (data) => {
        if (data && data.userId) {
            io.to(`user_${data.userId}`).emit('support_typing', { 
                userId: data.userId.toString(), 
                isTyping: Boolean(data.isTyping),
                name: data.name || "Suporte Cine GO!"
            });
        }
    });

    socket.on('mark_as_read', async (userId) => {
        const uId = typeof userId === 'string' ? userId : (userId && userId.userId ? userId.userId : null);
        if (!uId || !mongoose.Types.ObjectId.isValid(uId)) return;

        try {
            const now = new Date();
            await SupportMessage.updateMany(
                { userId: uId, status: { $ne: 'read' } },
                { $set: { status: 'read', readAt: now } }
            );

            io.to(`user_${uId}`).emit('messages_read', { userId: uId, status: 'read', readAt: now });
            io.to('admin_support_room').emit('messages_read', { userId: uId, status: 'read', readAt: now });
        } catch (err) {
            console.error('[Socket] Erro ao marcar como lido via Socket:', err);
        }
    });

    socket.on('get_support_status', async (userId) => {
        try {
            let activeAgent = null;
            for (let [agId, socketSet] of agentActiveSockets.entries()) {
                if (socketSet.size > 0) {
                    const agUser = await User.findById(agId);
                    if (agUser && agUser.studio) {
                        activeAgent = agUser;
                        break;
                    }
                }
            }

            if (activeAgent) {
                socket.emit('support_status', {
                    isOnline: true,
                    lastSeen: "",
                    agentName: activeAgent.name,
                    agentAvatar: activeAgent.avatar
                });
            } else {
                socket.emit('support_status', {
                    isOnline: false,
                    lastSeen: "Offline",
                    agentName: "Suporte Cine GO!",
                    agentAvatar: ""
                });
            }
        } catch (e) {
            console.error('[Socket] Erro ao buscar status do suporte:', e);
        }
    });

    socket.on('send_report', async (data) => {
        const { itemId, title, reason, userId } = data || {};
        try {
            if (itemId || reason) {
                const newReport = await Report.create({ itemId, title, reason, userId });
                io.to('admin_support_room').emit('new_report', newReport);
            }
        } catch (err) {
            console.error('[Socket] Erro ao salvar reporte no banco:', err);
        }
    });

    socket.on('disconnect', async () => {
        for (let [userId, socketSet] of userActiveSockets.entries()) {
            if (socketSet.has(socket.id)) {
                socketSet.delete(socket.id);
                if (socketSet.size === 0) {
                    await setOfflineUser(userId, io, false);
                }
                break;
            }
        }
        for (let [adminId, socketSet] of agentActiveSockets.entries()) {
            if (socketSet.has(socket.id)) {
                socketSet.delete(socket.id);
                await setOfflineUser(adminId, io, true);
                break;
            }
        }
    });
});

// Middleware de verificação de API KEY
function verifyApiKey(req, res, next) {
    const clientApiKey = req.headers['x-api-key'];
    if (!clientApiKey || clientApiKey !== API_KEY_SECRET) {
        return res.status(403).json({ error: 'Acesso negado: Chave de API inválida ou ausente.' });
    }
    next();
}

app.use(verifyApiKey);

// ================= SCHEMAS E MODELOS MONGOOSE =================
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

const ReportSchema = new mongoose.Schema({
    itemId: String,
    title: String,
    reason: String,
    userId: String,
    createdAt: { type: Date, default: Date.now }
});

const Report = mongoose.model('Report', ReportSchema);

// Conexão com o Banco de Dados
if (!MONGO_URL) {
    console.error('❌ [ERRO BANCO] MONGO_URL não foi informada no arquivo .env');
} else {
    mongoose.connect(MONGO_URL)
        .then(() => console.log('✅ MongoDB conectado com sucesso'))
        .catch(err => console.error('❌ Erro ao conectar no MongoDB:', err.message));
}

// Helpers de código de recuperação
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

// Middleware de Autenticação JWT
function auth(req, res, next) {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: 'Token de sessão ausente' });

    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        req.userId = decoded.id;
        next();
    } catch (err) {
        return res.status(401).json({ error: 'Token de sessão inválido ou expirado' });
    }
}

// Configuração de Upload R2 e Cloudinary
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

async function handleImageUpload(file) {
    if (STORAGE === "r2") {
        const ext = file.originalname.split('.').pop() || 'jpg';
        const fileName = `avatars/${Date.now()}-${Math.random().toString(36).substring(2)}.${ext}`;

        await r2.send(
            new PutObjectCommand({
                Bucket: process.env.R2_BUCKET,
                Key: fileName,
                Body: file.buffer,
                ContentType: file.mimetype,
                CacheControl: 'public, max-age=31536000'
            })
        );
        return `${process.env.R2_PUBLIC_URL}/${fileName}`;
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
            streamifier.createReadStream(file.buffer).pipe(stream);
        });
        return result.secure_url;
    }

    throw new Error("Storage não configurado corretamente");
}

app.post('/upload-avatar', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'Arquivo não enviado' });
        const url = await handleImageUpload(req.file);
        return res.json({ success: true, url });
    } catch (err) {
        console.error("UPLOAD ERROR:", err);
        return res.status(500).json({ error: "Erro no upload" });
    }
});

// ================= AUTENTICAÇÃO E PERFIL =================
app.post('/register', async (req, res) => {
    const { email, password, name, avatar, plan = 'FREE', profile = {} } = req.body;
    if (!email || !password || !name) return res.status(400).json({ error: 'Preencha todos os campos' });

    try {
        const cleanEmail = email.trim().toLowerCase();
        const exists = await User.findOne({ email: cleanEmail });
        if (exists) return res.status(400).json({ error: 'Email já cadastrado' });

        const hash = await bcrypt.hash(password, 10);
        const recoveryCode = await createUniqueRecoveryCode();

        const user = await User.create({ 
            email: cleanEmail, 
            password: hash, 
            name, 
            avatar, 
            plan: plan.toUpperCase(), 
            studio: false, 
            recoveryCode, 
            profile 
        });

        return res.json({
            success: true,
            recoveryCode,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan,
                studio: user.studio,
                recoveryCode: user.recoveryCode,
                profile: user.profile,
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        console.error("ERRO NO CADASTRO:", err);
        return res.status(500).json({ error: 'Erro interno no cadastro' });
    }
});

app.post('/login', async (req, res) => {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'Preencha e-mail e senha' });

    try {
        const cleanEmail = email.trim().toLowerCase();
        const user = await User.findOne({ email: cleanEmail });
        if (!user) return res.status(400).json({ error: 'Usuário não encontrado' });

        if (!user.password) return res.status(400).json({ error: 'Usuário sem senha cadastrada no banco' });

        const ok = await bcrypt.compare(password, user.password);
        if (!ok) return res.status(401).json({ error: 'Senha inválida' });

        const token = jwt.sign({ id: user._id }, JWT_SECRET, { expiresIn: '30d' });

        return res.json({
            token,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                studio: user.studio !== undefined ? user.studio : false,
                recoveryCode: user.recoveryCode,
                profile: user.profile || {},
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        console.error("ERRO NO LOGIN:", err);
        return res.status(500).json({ error: 'Erro interno no servidor', details: err.message });
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
                studio: user.studio !== undefined ? user.studio : false,
                recoveryCode: user.recoveryCode,
                profile: user.profile || {},
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.get('/api/user/profile', auth, async (req, res) => {
    try {
        const user = await User.findById(req.userId).select('-password');
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        return res.json({
            success: true,
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                studio: user.studio !== undefined ? user.studio : false,
                recoveryCode: user.recoveryCode,
                profile: user.profile || {},
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        return res.status(500).json({ error: 'Erro interno ao buscar perfil' });
    }
});

// ================= GERENCIAMENTO DO PAINEL ADMIN / STUDIO =================

// 1. Listar todos os usuários para o aplicativo de Administração
app.get('/admin/users', auth, async (req, res) => {
    try {
        const admin = await User.findById(req.userId);
        if (!admin || !admin.studio) {
            return res.status(403).json({ error: 'Acesso negado: Permissão de Studio necessária.' });
        }

        const { search } = req.query;
        let filter = {};

        if (search) {
            const regex = new RegExp(search.trim(), 'i');
            filter = { $or: [{ name: regex }, { email: regex }] };
        }

        const users = await User.find(filter).select('-password').sort({ createdAt: -1 });
        return res.json({ success: true, count: users.length, users });
    } catch (err) {
        console.error('[ADMIN LIST ERRO]:', err);
        return res.status(500).json({ error: 'Erro ao listar usuários' });
    }
});

// 2. Editar qualquer perfil de usuário através do Painel Admin
app.put('/admin/user/:id', auth, async (req, res) => {
    try {
        const admin = await User.findById(req.userId);
        if (!admin || !admin.studio) {
            return res.status(403).json({ error: 'Acesso negado: Apenas administradores podem editar contas.' });
        }

        const { name, email, avatar, plan, studio, profile } = req.body;
        const updateFields = {};

        if (name !== undefined) updateFields.name = name.trim();
        if (email !== undefined) updateFields.email = email.trim().toLowerCase();
        if (avatar !== undefined) updateFields.avatar = avatar;
        if (plan !== undefined) updateFields.plan = plan.toString().toUpperCase();
        if (studio !== undefined) updateFields.studio = Boolean(studio);
        if (profile !== undefined) updateFields.profile = profile;

        const updatedUser = await User.findByIdAndUpdate(
            req.params.id,
            { $set: updateFields },
            { new: true }
        ).select('-password');

        if (!updatedUser) {
            return res.status(404).json({ error: 'Usuário não encontrado' });
        }

        return res.json({
            success: true,
            message: 'Perfil de usuário atualizado com sucesso!',
            user: updatedUser
        });
    } catch (err) {
        console.error('[ADMIN EDIT USER ERRO]:', err);
        return res.status(500).json({ error: 'Erro ao editar usuário via painel admin' });
    }
});

// 3. Deletar usuário via Painel Admin
app.delete('/admin/user/:id', auth, async (req, res) => {
    try {
        const admin = await User.findById(req.userId);
        if (!admin || !admin.studio) {
            return res.status(403).json({ error: 'Acesso negado' });
        }

        const deleted = await User.findByIdAndDelete(req.params.id);
        if (!deleted) return res.status(404).json({ error: 'Usuário não encontrado' });

        return res.json({ success: true, message: 'Usuário removido com sucesso' });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao deletar usuário' });
    }
});

// Inicialização
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Servidor unificado do Prime Studio rodando na porta ${PORT}`);
});
