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
        studio: user.studio !== undefined ? user.studio : false,
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
    studio: { type: Boolean, default: false }, // Tag única de restrição: padrão false
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

// Conexão com MongoDB
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
                    studio: true, // Apenas para teste inicial do chat
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

// ================= UPLOAD AVATAR =================
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
        console.error("UPLOAD ERROR:", err);
        return res.status(500).json({ error: "Erro no upload" });
    }
});

// ================= ROTAS DE AUTENTICAÇÃO E PERFIL =================
app.post('/register', async (req, res) => {
    // Toda nova conta nasce por padrão com studio = false
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
            studio: false, // Forçado obrigatoriamente como false no cadastro
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
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
        return res.status(500).json({ error: 'Erro interno' });
    }
});

// Rota de login enviando o parâmetro 'studio' para checagem rigorosa
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
                studio: user.studio !== undefined ? user.studio : false,
                recoveryCode: user.recoveryCode,
                profile: user.profile
            }
        });
    } catch (err) {
        console.error(err);
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

// ================= VERIFY PURCHASES (GOOGLE PLAY) =================
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

// ================= SENHAS E SEGURANÇA =================
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

// ================= ADMIN: ALTERAR PLANO =================
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

// ================= ROTAS DE SUPORTE (CHAT) =================
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
        if (!requestingUser || !requestingUser.studio) {
            return res.status(403).json({ error: 'Acesso negado. Apenas estúdio autorizado.' });
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
            select: 'name email avatar plan studio'
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

        if (sender.studio && targetUserId) {
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
    console.log(`Servidor unificado do Prime Studio rodando na porta ${PORT}`);
});
