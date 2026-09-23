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

// ================= SUPORTE A CONFIGURAÇÃO EM BASE64 =================
if (process.env.CONFIG_BASE64) {
    try {
        const decodedConfig = Buffer.from(process.env.CONFIG_BASE64, 'base64').toString('utf8');
        const parsedConfig = JSON.parse(decodedConfig);
        Object.assign(process.env, parsedConfig);
        console.log('✅ Configurações carregadas via Base64 com sucesso.');
    } catch (err) {
        console.error('❌ Erro ao decodificar CONFIG_BASE64:', err.message);
    }
}

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*" }
});

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
    return res.status(200).json({ status: 'online', message: 'Servidor Unificado Prime Studio em execução' });
});

// Mapas de controle separados para usuários comuns e agentes de suporte (studio: true)
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
        console.log(`[REPORTE SOCKET] ID: ${itemId} | Título: ${title} | Motivo: ${reason} | Usuário: ${userId}`);

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

const ReportSchema = new mongoose.Schema({
    itemId: String,
    title: String,
    reason: String,
    userId: String,
    createdAt: { type: Date, default: Date.now }
});

const Report = mongoose.model('Report', ReportSchema);

mongoose.connect(process.env.MONGO_URL)
.then(() => console.log('MongoDB conectado com sucesso'))
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

// ================= MULTER COM TRAVA DE SEGURANÇA =================
const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 }, // Aumentado limite para 5MB
    fileFilter: (req, file, cb) => {
        const allowedMime = ['image/jpeg', 'image/png', 'image/webp', 'image/jpg'];
        if (allowedMime.includes(file.mimetype)) {
            cb(null, true);
        } else {
            cb(new Error('Apenas imagens em formato JPG, PNG ou WEBP são permitidas.'));
        }
    }
});

// Função auxiliar reutilizável para upload no storage
async function uploadToStorage(file) {
    if (STORAGE === "r2") {
        const ext = file.originalname ? file.originalname.split('.').pop() : 'jpg';
        const fileName = `avatars/${Date.now()}-${Math.random().toString(36).substring(2)}.${ext}`;

        await r2.send(
            new PutObjectCommand({
                Bucket: process.env.R2_BUCKET,
                Key: fileName,
                Body: file.buffer,
                ContentType: file.mimetype || 'image/jpeg',
                CacheControl: 'public, max-age=31536000'
            })
        );
        const baseUrl = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');
        return `${baseUrl}/${fileName}`;
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

    throw new Error("Provedor de armazenamento não configurado no servidor");
}

app.post('/upload-avatar', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) return res.status(400).json({ error: 'Arquivo não enviado' });
        const uploadedUrl = await uploadToStorage(req.file);
        return res.json({ success: true, url: uploadedUrl });
    } catch (err) {
        console.error("UPLOAD ERROR:", err);
        return res.status(500).json({ error: err.message || "Erro no upload" });
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

        const user = await User.create({ 
            email, 
            password: hash, 
            name, 
            avatar, 
            plan, 
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

        const token = jwt.sign({ id: user._id }, process.env.JWT_SECRET, { expiresIn: '30d' });

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
                profile: user.profile,
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
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
                studio: user.studio !== undefined ? user.studio : false,
                recoveryCode: user.recoveryCode,
                profile: user.profile || {},
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        console.error("ERRO EM /ME:", err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

// ================= ROTA DE EDIÇÃO DE PERFIL UNIFICADA (TEXTO + IMAGEM) =================
app.put('/profile', auth, upload.single('avatar'), async (req, res) => {
    try {
        const user = await User.findById(req.userId);
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        const { name, bio, avatarUrl } = req.body;
        let profileData = {};

        if (req.body.profile) {
            try {
                profileData = typeof req.body.profile === 'string' 
                    ? JSON.parse(req.body.profile) 
                    : req.body.profile;
            } catch (e) {
                profileData = {};
            }
        }

        if (bio !== undefined && bio !== null) {
            profileData.bio = bio;
        }

        // 1. Atualização do Nome
        if (typeof name === 'string' && name.trim()) {
            user.name = name.trim();
        }

        // 2. Processamento do Avatar (Arquivo enviado via Multipart OU URL enviada via Body)
        if (req.file) {
            const uploadedAvatarUrl = await uploadToStorage(req.file);
            user.avatar = uploadedAvatarUrl;
        } else if (typeof avatarUrl === 'string' && avatarUrl.trim()) {
            user.avatar = avatarUrl.trim();
        }

        // 3. Atualização e Persistência Garantida do Objeto profile no Mongoose
        const currentProfile = (typeof user.profile === 'object' && user.profile !== null) ? user.profile : {};
        user.profile = { ...currentProfile, ...profileData };
        user.markModified('profile'); // <--- CRUCIAL PARA SALVAR CAMPOS MIXED NO MONGOOSE

        await user.save();

        return res.json({
            success: true,
            message: 'Perfil atualizado com sucesso!',
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                recoveryCode: user.recoveryCode,
                profile: user.profile,
                isOnline: user.isOnline,
                lastSeen: user.lastSeen
            }
        });
    } catch (err) {
        console.error("ERRO AO ATUALIZAR PERFIL:", err);
        return res.status(500).json({ error: err.message || 'Erro ao atualizar perfil' });
    }
});

app.get('/user/presence/:userId', auth, async (req, res) => {
    try {
        const user = await User.findById(req.params.userId).select('isOnline lastSeen name avatar');
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        return res.json({
            success: true,
            presence: {
                isOnline: user.isOnline || false,
                lastSeen: user.lastSeen || null
            }
        });
    } catch (err) {
        return res.status(500).json({ error: 'Erro ao buscar presença' });
    }
});

app.post('/verify-purchase', auth, async (req, res) => {
    const { purchaseToken } = req.body;
    if (!purchaseToken) return res.status(400).json({ error: 'Token de compra não enviado' });

    try {
        const user = await User.findByIdAndUpdate(
            req.userId,
            { plan: 'VIP', purchaseToken: purchaseToken },
            { new: true }
        );

        return res.json({
            success: true,
            message: 'Plano atualizado para VIP com sucesso!',
            plan: user.plan
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao processar compra' });
    }
});

app.post('/verify-password', auth, async (req, res) => {
    const { currentPassword } = req.body;
    if (!currentPassword) return res.status(400).json({ error: 'Senha não informada' });

    try {
        const user = await User.findById(req.userId);
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        const valid = await bcrypt.compare(currentPassword, user.password);
        return res.json({ valid });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/change-password', auth, async (req, res) => {
    const { currentPassword, newPassword } = req.body;
    if (!currentPassword || !newPassword) return res.status(400).json({ error: 'Preencha todos os campos' });

    try {
        const user = await User.findById(req.userId);
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        const ok = await bcrypt.compare(currentPassword, user.password);
        if (!ok) return res.status(401).json({ error: 'Senha atual incorreta' });

        user.password = await bcrypt.hash(newPassword, 10);
        await user.save();

        return res.json({ success: true, message: 'Senha alterada com sucesso' });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/change-email', auth, async (req, res) => {
    const { currentPassword, newEmail } = req.body;
    if (!currentPassword || !newEmail) return res.status(400).json({ error: 'Preencha todos os campos' });

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(newEmail)) {
        return res.status(400).json({ error: 'E-mail inválido' });
    }

    try {
        const user = await User.findById(req.userId);
        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        const ok = await bcrypt.compare(currentPassword, user.password);
        if (!ok) return res.status(401).json({ error: 'Senha atual incorreta' });

        const exists = await User.findOne({ email: newEmail });
        if (exists && exists._id.toString() !== user._id.toString()) {
            return res.status(400).json({ error: 'Este e-mail já está em uso' });
        }

        user.email = newEmail.trim();
        await user.save();

        return res.json({
            success: true,
            message: 'E-mail alterado com sucesso',
            user: {
                id: user._id,
                name: user.name,
                email: user.email,
                avatar: user.avatar,
                plan: user.plan || 'FREE',
                recoveryCode: user.recoveryCode,
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.post('/recover-with-code', async (req, res) => {
    const { email, recoveryCode, newPassword } = req.body;
    if (!email || !recoveryCode || !newPassword) return res.status(400).json({ error: 'Preencha todos os campos' });

    try {
        const user = await User.findOne({ email, recoveryCode });
        if (!user) return res.status(400).json({ error: 'Código inválido' });

        user.password = await bcrypt.hash(newPassword, 10);
        await user.save();

        return res.json({ success: true, message: 'Senha redefinida com sucesso' });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/admin/change-plan', async (req, res) => {
    const { email, plan } = req.body;
    if (!email || !plan) return res.status(400).json({ error: 'Preencha e-mail e plano' });

    try {
        const user = await User.findOneAndUpdate(
            { email },
            { plan: plan.toUpperCase() },
            { new: true }
        );

        if (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        return res.json({
            success: true,
            message: `Plano alterado para ${user.plan}`,
            user: { id: user._id, email: user.email, plan: user.plan }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao alterar plano' });
    }
});

// ================= ROTAS DE SUPORTE =================
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
        console.error(err);
        return res.status(500).json({ error: 'Erro ao buscar mensagens' });
    }
});

app.get('/support/admin/chats', auth, async (req, res) => {
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser || !requestingUser.studio) {
            return res.status(403).json({ error: 'Acesso negado. Apenas estúdio autorizado.' });
        }

        const chats = await SupportMessage.aggregate([
            { $sort: { createdAt: -1 } },             {$group: {
                    _id: "$userId",
                    lastMessage: { $first: "$message" },
                    lastMessageDate: { $first: "$createdAt" },
                    lastMessageStatus: { $first: "$status" },
                    lastMessageSenderModel: { $first: "$senderModel" },
                    unreadCount: {
                        $sum: {$cond: [
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
    } catch (err)  {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao listar chats' });
    }
});

app.post('/support/message', auth, async (req, res) => {
    try {
        const { message, targetUserId } = req.body;
        const senderId = req.userId;

        const sender = await User.findById(senderId);
        if (!sender) return res.status(404).json({ error: 'Usuário não encontrado' });

        const isAdmin = sender.studio === true; 

        const newMessage = await SupportMessage.create({
            userId: isAdmin ? targetUserId : senderId,
            senderId,
            senderModel: isAdmin ? 'admin' : 'user',
            message,
            status: 'sent',
            createdAt: new Date()
        });

        const populatedMessage = await SupportMessage.findById(newMessage._id)
            .populate('senderId', 'name avatar email isOnline lastSeen');

        if (isAdmin) {
            io.to(`user_${targetUserId}`).emit('new_support_message', populatedMessage);
            io.to('admin_support_room').emit('new_support_message', populatedMessage);
        } else {
            io.to(`user_${senderId}`).emit('new_support_message', populatedMessage);
            io.to('admin_support_room').emit('new_support_message', populatedMessage);
        }

        return res.status(200).json({ success: true, message: populatedMessage });
    } catch (error) {
        console.error(error);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/support/read', auth, async (req, res) => {
    const { targetUserId } = req.body;
    try {
        const requestingUser = await User.findById(req.userId);
        if (!requestingUser) return res.status(404).json({ error: 'Usuário não encontrado' });

        let chatUserId = req.userId;
        if (requestingUser.studio && targetUserId) {
            chatUserId = targetUserId;
        }

        const now = new Date();
        await SupportMessage.updateMany(
            { userId: chatUserId, senderId: { $ne: req.userId }, status: {$ne: 'read' } },
            { $set: { status: 'read', readAt: now } }
        );

        io.to(`user_${chatUserId}`).emit('messages_read', { userId: chatUserId, status: 'read', readAt: now });
        io.to('admin_support_room').emit('messages_read', { userId: chatUserId, status: 'read', readAt: now });

        return res.json({ success: true, readAt: now });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro ao marcar mensagens como lidas' });
    }
});

// ================= ROTAS DE REPORTES =================
app.get('/api/reports', async (req, res) => {
    try {
        const reports = await Report.find().sort({ createdAt: -1 });
        return res.status(200).json({ success: true, reports });
    } catch (err) {
        console.error('Erro ao listar reportes:', err);
        return res.status(500).json({ error: 'Erro interno ao listar reportes' });
    }
});

app.post('/api/report', async (req, res) => {
    try {
        const { itemId, title, reason, userId } = req.body;
        console.log(`[REPORTE HTTP] ID: ${itemId} | Título: ${title} | Motivo: ${reason} | Usuário: ${userId}`);

        const newReport = await Report.create({ itemId, title, reason, userId });

        io.to('admin_support_room').emit('new_report', newReport);

        return res.status(200).json({ success: true, message: 'Reporte salvo com sucesso!', report: newReport });
    } catch (err) {
        console.error('Erro ao processar reporte:', err);
        return res.status(500).json({ error: 'Erro interno ao salvar reporte' });
    }
});

app.delete('/api/reports/:id', async (req, res) => {
    try {
        const { id } = req.params;
        if (!mongoose.Types.ObjectId.isValid(id)) {
            return res.status(400).json({ error: 'ID de reporte inválido' });
        }

        const deleted = await Report.findByIdAndDelete(id);
        if (!deleted) {
            return res.status(404).json({ error: 'Reporte não encontrado' });
        }

        return res.status(200).json({ success: true, message: 'Reporte deletado com sucesso!' });
    } catch (err) {
        console.error('Erro ao deletar reporte:', err);
        return res.status(500).json({ error: 'Erro interno ao deletar reporte' });
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor unificado do Prime Studio rodando na porta ${PORT}`);
});
