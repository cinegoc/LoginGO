Const express = require('express');
Const mongoose = require('mongoose');
Const bcrypt = require('bcryptjs');
Const jwt = require('jsonwebtoken');
Const cors = require('cors');
Const multer = require('multer');
Const http = require('http');
Const { Server } = require('socket.io');

Const {
    S3Client,
    PutObjectCommand
} = require('@aws-sdk/client-s3');

Const cloudinary = require('cloudinary').v2;
Const streamifier = require('streamifier');

Require('dotenv').config();

// ================= CONFIGURAÇÕES E VARIÁVEIS DE AMBIENTE =================
Const PORT = process.env.PORT || 3000;
Const MONGO_URL = process.env.MONGO_URL || process.env.MONGODB_URI;
Const JWT_SECRET = process.env.JWT_SECRET || "CHAVE_SECRETA_CINEGO_2026";
Const API_KEY_SECRET = process.env.APP_API_KEY || "SUA_CHAVE_MESTRA_STREAMING_2026";
Const STORAGE = process.env.STORAGE || "r2";

Const app = express();
Const server = http.createServer(app);

Const io = new Server(server, {
    cors: { origin: "*" }
});

App.use(cors());
App.use(express.json());

// Rota de Health Check
app.get('/', (req, res) => {
    Return res.status(200).json({ status: 'online', message: 'Servidor Unificado Prime Studio em execução' });
});

// Mapas de controle de conexões Socket para usuários e agentes
Const userActiveSockets = new Map();
Const agentActiveSockets = new Map();

Async function setOfflineUser(userId, ioInstance, isAgent = false) {
    If (!userId || !mongoose.Types.ObjectId.isValid(userId)) return;
    Const uIdStr = userId.toString();

    If (isAgent) {
        AgentActiveSockets.delete(uIdStr);
    } else {
        UserActiveSockets.delete(uIdStr);
    }

    Const lastSeen = new Date();
    Try {
        Const user = await User.findById(uIdStr);
        If (!user) return;

        If (user.studio) {
            Const totalAgentSockets = Array.from(agentActiveSockets.values()).reduce((acc, set) => acc + set.size, 0);
            If (totalAgentSockets > 0) return; 
        }

        Await User.findByIdAndUpdate(uIdStr, { isOnline: false, lastSeen });
        Const formattedLastSeen = lastSeen.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        If (user.studio) {
            IoInstance.emit('support_status', { 
                UserId: uIdStr, 
                IsOnline: false, 
                LastSeen: formattedLastSeen,
                AgentName: "",
                AgentAvatar: ""
            });
        } else {
            IoInstance.to('admin_support_room').emit('user_status_changed', { userId: uIdStr, isOnline: false, lastSeen: formattedLastSeen });
            IoInstance.to(uIdStr).emit('user_status_changed', { userId: uIdStr, isOnline: false, lastSeen: formattedLastSeen });
        }
    } catch (e) {
        Console.error('[Socket] Erro crítico ao persistir status offline:', e);
    }
}

// ================= TEMPO REAL (SOCKET.IO) =================
io.on('connection', (socket) => {

    Socket.on('join_user_room', async (userId) => {
        If (userId && mongoose.Types.ObjectId.isValid(userId)) {
            Const uIdStr = userId.toString();
            Socket.join(`user_${uIdStr}`);
            Socket.join(uIdStr);

            If (!userActiveSockets.has(uIdStr)) {
                UserActiveSockets.set(uIdStr, new Set());
            }
            UserActiveSockets.get(uIdStr).add(socket.id);

            Try {
                Const user = await User.findById(uIdStr);
                If (user && !user.studio) {
                    Await User.findByIdAndUpdate(uIdStr, { isOnline: true });
                    Io.to('admin_support_room').emit('user_status_changed', { userId: uIdStr, isOnline: true, lastSeen: "" });
                    Io.to(`user_${uIdStr}`).emit('user_status_changed', { userId: uIdStr, isOnline: true, lastSeen: "" });
                }
            } catch (e) {
                Console.error('[Socket] Erro ao atualizar status online do usuário:', e);
            }
        }
    });

    Socket.on('join_admin_support', async (adminId) => {
        Const targetId = adminId || (socket.handshake.auth && socket.handshake.auth.userId);
        Socket.join('admin_support_room');

        If (targetId && mongoose.Types.ObjectId.isValid(targetId)) {
            Const uIdStr = targetId.toString();
            Socket.join(`user_${uIdStr}`);
            Socket.join(uIdStr);

            If (!agentActiveSockets.has(uIdStr)) {
                AgentActiveSockets.set(uIdStr, new Set());
            }
            AgentActiveSockets.get(uIdStr).add(socket.id);

            Try {
                Const adminUser = await User.findById(uIdStr);
                If (adminUser && adminUser.studio) {
                    Await User.findByIdAndUpdate(uIdStr, { isOnline: true });
                    Io.emit('support_status', { 
                        UserId: uIdStr, 
                        IsOnline: true, 
                        LastSeen: "",
                        AgentName: adminUser.name,
                        AgentAvatar: adminUser.avatar
                    });
                }
            } catch (e) {
                Console.error('[Socket] Erro ao atualizar status online do admin:', e);
            }
        }
    });

    Socket.on('support_status', (data) => {
        If (data) {
            Io.emit('support_status', data);
        }
    });

    Socket.on('user_typing', (data) => {
        If (data && data.userId) {
            Const payload = { 
                UserId: data.userId.toString(), 
                IsTyping: Boolean(data.isTyping) 
            };
            Io.to('admin_support_room').emit('user_typing', payload);
            Io.to('admin_support_room').emit('typing_status', payload);
        }
    });

    Socket.on('support_typing', (data) => {
        If (data && data.userId) {
            Io.to(`user_${data.userId}`).emit('support_typing', { 
                UserId: data.userId.toString(), 
                IsTyping: Boolean(data.isTyping),
                Name: data.name || "Suporte Cine GO!"
            });
        }
    });

    Socket.on('mark_as_read', async (userId) => {
        Const uId = typeof userId === 'string' ? userId : (userId && userId.userId ? userId.userId : null);
        If (!uId || !mongoose.Types.ObjectId.isValid(uId)) return;

        Try {
            Const now = new Date();
            Await SupportMessage.updateMany(
                { userId: uId, status: { $ne: 'read' } },
                { $set: { status: 'read', readAt: now } }
            );

            Io.to(`user_${uId}`).emit('messages_read', { userId: uId, status: 'read', readAt: now });
            Io.to('admin_support_room').emit('messages_read', { userId: uId, status: 'read', readAt: now });
        } catch (err) {
            Console.error('[Socket] Erro ao marcar como lido via Socket:', err);
        }
    });

    Socket.on('get_support_status', async (userId) => {
        Try {
            Let activeAgent = null;
            For (let [agId, socketSet] of agentActiveSockets.entries()) {
                If (socketSet.size > 0) {
                    Const agUser = await User.findById(agId);
                    If (agUser && agUser.studio) {
                        ActiveAgent = agUser;
                        Break;
                    }
                }
            }

            If (activeAgent) {
                Socket.emit('support_status', {
                    IsOnline: true,
                    LastSeen: "",
                    AgentName: activeAgent.name,
                    AgentAvatar: activeAgent.avatar
                });
            } else {
                Socket.emit('support_status', {
                    IsOnline: false,
                    LastSeen: "Offline",
                    AgentName: "Suporte Cine GO!",
                    AgentAvatar: ""
                });
            }
        } catch (e) {
            Console.error('[Socket] Erro ao buscar status do suporte:', e);
        }
    });

    Socket.on('send_report', async (data) => {
        Const { itemId, title, reason, userId } = data || {};
        Try {
            If (itemId || reason) {
                Const newReport = await Report.create({ itemId, title, reason, userId });
                Io.to('admin_support_room').emit('new_report', newReport);
            }
        } catch (err) {
            Console.error('[Socket] Erro ao salvar reporte no banco:', err);
        }
    });

    Socket.on('disconnect', async () => {
        For (let [userId, socketSet] of userActiveSockets.entries()) {
            If (socketSet.has(socket.id)) {
                SocketSet.delete(socket.id);
                If (socketSet.size === 0) {
                    Await setOfflineUser(userId, io, false);
                }
                Break;
            }
        }
        For (let [adminId, socketSet] of agentActiveSockets.entries()) {
            If (socketSet.has(socket.id)) {
                SocketSet.delete(socket.id);
                Await setOfflineUser(adminId, io, true);
                Break;
            }
        }
    });
});

// Middleware de verificação de API KEY
function verifyApiKey(req, res, next) {
    Const clientApiKey = req.headers['x-api-key'];
    If (!clientApiKey || clientApiKey !== API_KEY_SECRET) {
        Return res.status(403).json({ error: 'Acesso negado: Chave de API inválida ou ausente.' });
    }
    Next();
}

App.use(verifyApiKey);

// ================= SCHEMAS E MODELOS MONGOOSE =================
Const UserSchema = new mongoose.Schema({
    Email: { type: String, unique: true },
    Password: String,
    Name: String,
    Avatar: String,
    Plan: { type: String, default: 'FREE' },
    Studio: { type: Boolean, default: false },
    PurchaseToken: { type: String, default: null },
    RecoveryCode: { type: String, unique: true },
    Profile: { type: mongoose.Schema.Types.Mixed, default: {} },
    IsOnline: { type: Boolean, default: false },
    LastSeen: { type: Date, default: Date.now },
    CreatedAt: { type: Date, default: Date.now }
});

Const User = mongoose.model('User', UserSchema);

Const SupportMessageSchema = new mongoose.Schema({
    UserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    SenderId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    SenderModel: { type: String, enum: ['user', 'admin'], required: true },
    Message: { type: String, required: true },
    Status: { type: String, enum: ['sent', 'delivered', 'read'], default: 'sent' },
    ReadAt: { type: Date, default: null },
    CreatedAt: { type: Date, default: Date.now }
});

Const SupportMessage = mongoose.model('SupportMessage', SupportMessageSchema);

Const ReportSchema = new mongoose.Schema({
    ItemId: String,
    Title: String,
    Reason: String,
    UserId: String,
    CreatedAt: { type: Date, default: Date.now }
});

Const Report = mongoose.model('Report', ReportSchema);

// Conexão com o Banco de Dados
If (!MONGO_URL) {
    Console.error('❌ [ERRO BANCO] MONGO_URL não foi informada no arquivo .env');
} else {
    Mongoose.connect(MONGO_URL)
        .then(() => console.log('✅ MongoDB conectado com sucesso'))
        .catch(err => console.error('❌ Erro ao conectar no MongoDB:', err.message));
}

function generateRecoveryCode() {
    Const a = Math.floor(1000 + Math.random() * 9000);
    Const b = Math.floor(1000 + Math.random() * 9000);
    Return `SG-${a}-${b}`;
}

Async function createUniqueRecoveryCode() {
    Let code;
    Do {
        Code = generateRecoveryCode();
    } while (await User.findOne({ recoveryCode: code }));
    Return code;
}

// Middleware de Autenticação JWT
function auth(req, res, next) {
    Const token = req.headers.authorization?.replace('Bearer ', '');
    If (!token) return res.status(401).json({ error: 'Token de sessão ausente' });

    Try {
        Const decoded = jwt.verify(token, JWT_SECRET);
        Req.userId = decoded.id;
        Next();
    } catch (err) {
        Return res.status(401).json({ error: 'Token de sessão inválido ou expirado' });
    }
}

// Configuração do Multer e Storage R2 / Cloudinary
Const r2 = new S3Client({
    Region: 'auto',
    Endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    Credentials: {
        AccessKeyId: process.env.R2_ACCESS_KEY_ID,
        SecretAccessKey: process.env.R2_SECRET_ACCESS_KEY
    }
});

Cloudinary.config({
    Cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    Api_key: process.env.CLOUDINARY_API_KEY,
    Api_secret: process.env.CLOUDINARY_API_SECRET
});

Const upload = multer({ storage: multer.memoryStorage() });

Async function handleImageUpload(file) {
    If (STORAGE === "r2") {
        Const ext = file.originalname.split('.').pop() || 'jpg';
        Const fileName = `avatars/${Date.now()}-${Math.random().toString(36).substring(2)}.${ext}`;

        Await r2.send(
            New PutObjectCommand({
                Bucket: process.env.R2_BUCKET,
                Key: fileName,
                Body: file.buffer,
                ContentType: file.mimetype,
                CacheControl: 'public, max-age=31536000'
            })
        );
        Return `${process.env.R2_PUBLIC_URL}/${fileName}`;
    }

    If (STORAGE === "cloudinary") {
        Const result = await new Promise((resolve, reject) => {
            Const stream = cloudinary.uploader.upload_stream(
                { folder: "avatars" },
                (error, result) => {
                    If (error) reject(error);
                    Else resolve(result);
                }
            );
            Streamifier.createReadStream(file.buffer).pipe(stream);
        });
        Return result.secure_url;
    }

    Throw new Error("Storage não configurado corretamente");
}

App.post('/upload-avatar', upload.single('file'), async (req, res) => {
    Try {
        If (!req.file) return res.status(400).json({ error: 'Arquivo não enviado' });
        Const url = await handleImageUpload(req.file);
        Return res.json({ success: true, url });
    } catch (err) {
        Console.error("UPLOAD ERROR:", err);
        Return res.status(500).json({ error: "Erro no upload" });
    }
});

// ================= AUTENTICAÇÃO E CONTA DE USUÁRIO =================
App.post('/register', async (req, res) => {
    Const { email, password, name, avatar, plan = 'FREE', profile = {} } = req.body;
    If (!email || !password || !name) return res.status(400).json({ error: 'Preencha todos os campos' });

    Try {
        Const cleanEmail = email.trim().toLowerCase();
        Const exists = await User.findOne({ email: cleanEmail });
        If (exists) return res.status(400).json({ error: 'Email já cadastrado' });

        Const hash = await bcrypt.hash(password, 10);
        Const recoveryCode = await createUniqueRecoveryCode();

        Const user = await User.create({ 
            Email: cleanEmail, 
            Password: hash, 
            Name, 
            Avatar, 
            Plan, 
            Studio: false, 
            RecoveryCode, 
            Profile 
        });

        Return res.json({
            Success: true,
            RecoveryCode,
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan,
                Studio: user.studio,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile,
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error("ERRO NO CADASTRO:", err);
        Return res.status(500).json({ error: 'Erro interno no cadastro' });
    }
});

App.post('/login', async (req, res) => {
    Const { email, password } = req.body;
    If (!email || !password) {
        Return res.status(400).json({ error: 'Preencha e-mail e senha' });
    }

    Try {
        Const cleanEmail = email.trim().toLowerCase();
        Const user = await User.findOne({ email: cleanEmail });
        If (!user) return res.status(400).json({ error: 'Usuário não encontrado' });

        If (!user.password) {
            Return res.status(400).json({ error: 'Usuário sem senha cadastrada no banco' });
        }

        Const ok = await bcrypt.compare(password, user.password);
        If (!ok) return res.status(401).json({ error: 'Senha inválida' });

        Const token = jwt.sign({ id: user._id }, JWT_SECRET, { expiresIn: '30d' });

        Return res.json({
            Token,
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio !== undefined ? user.studio : false,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile || {},
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error("ERRO NO LOGIN:", err);
        Return res.status(500).json({ error: 'Erro interno no servidor', details: err.message });
    }
});

// Rota pública para o aplicativo de música buscar os dados do criador pelo ID (Removido o auth para evitar bloqueio)
app.get('/user/:id', async (req, res) => {
    Try {
        Const user = await User.findById(req.params.id).select('name avatar plan studio');
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({
            Success: true,
            Id: user._id,
            Name: user.name,
            Avatar: user.avatar,
            Plan: user.plan,
            Studio: user.studio
        });
    } catch (err) {
        Console.error("ERRO AO BUSCAR CRIADOR:", err);
        Return res.status(500).json({ error: 'Erro interno ao buscar criador' });
    }
});


app.get('/me', auth, async (req, res) => {
    Try {
        Const user = await User.findById(req.userId).select('-password');
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio !== undefined ? user.studio : false,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile || {},
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro interno' });
    }
});

// ================= ROTAS DE PERFIL COMPATÍVEIS COM O APP ANDROID =================

app.get('/api/user/profile', auth, async (req, res) => {
    Try {
        Const user = await User.findById(req.userId).select('-password');
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({
            Success: true,
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio !== undefined ? user.studio : false,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile || {},
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error("ERRO GET PERFIL:", err);
        Return res.status(500).json({ error: 'Erro interno ao buscar perfil' });
    }
});

app.put('/profile', auth, async (req, res) => {
    Const { name, avatar, profile = {} } = req.body;
    Try {
        Const user = await User.findById(req.userId);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        If (typeof name === 'string' && name.trim()) user.name = name.trim();
        If (typeof avatar === 'string' && avatar.trim()) user.avatar = avatar;

        User.profile = { ...user.profile, ...profile };
        Await user.save();

        Return res.json({
            Success: true,
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio !== undefined ? user.studio : false,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile,
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error("ERRO AO ATUALIZAR PERFIL:", err);
        Return res.status(500).json({ error: 'Erro interno ao salvar perfil' });
    }
});

app.put('/api/user/profile', auth, upload.single('avatar'), async (req, res) => {
    Try {
        Const { name, bio, profile } = req.body;
        Const user = await User.findById(req.userId);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        If (typeof name === 'string' && name.trim()) {
            User.name = name.trim();
        }

        Let userBio = bio || "";
        If (profile) {
            Try {
                Const parsed = typeof profile === 'string' ? JSON.parse(profile) : profile;
                If (parsed.bio) userBio = parsed.bio;
            } catch (e) {
                UserBio = profile;
            }
        }

        User.profile = { ...(user.profile || {}), bio: userBio };

        If (req.file) {
            User.avatar = await handleImageUpload(req.file);
        }

        Await user.save();

        Return res.json({
            Success: true,
            Message: 'Perfil atualizado com sucesso!',
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio !== undefined ? user.studio : false,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile,
                IsOnline: user.isOnline,
                LastSeen: user.lastSeen
            }
        });
    } catch (err) {
        Console.error("ERRO AO SALVAR PERFIL COMPLETO:", err);
        Return res.status(500).json({ error: 'Erro interno ao salvar perfil' });
    }
});

app.get('/user/presence/:userId', auth, async (req, res) => {
    Try {
        Const user = await User.findById(req.params.userId).select('isOnline lastSeen name avatar');
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({
            Success: true,
            Presence: {
                IsOnline: user.isOnline || false,
                LastSeen: user.lastSeen || null
            }
        });
    } catch (err) {
        Return res.status(500).json({ error: 'Erro ao buscar presença' });
    }
});

app.post('/verify-purchase', auth, async (req, res) => {
    Const { purchaseToken } = req.body;
    If (!purchaseToken) return res.status(400).json({ error: 'Token de compra não enviado' });

    Try {
        Const user = await User.findByIdAndUpdate(
            Req.userId,
            { plan: 'VIP', purchaseToken: purchaseToken },
            { new: true }
        );

        Return res.json({
            Success: true,
            Message: 'Plano atualizado para VIP com sucesso!',
            Plan: user.plan
        });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao processar compra' });
    }
});

app.post('/verify-password', auth, async (req, res) => {
    Const { currentPassword } = req.body;
    If (!currentPassword) return res.status(400).json({ error: 'Senha não informada' });

    Try {
        Const user = await User.findById(req.userId);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Const valid = await bcrypt.compare(currentPassword, user.password);
        Return res.json({ valid });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/change-password', auth, async (req, res) => {
    Const { currentPassword, newPassword } = req.body;
    If (!currentPassword || !newPassword) return res.status(400).json({ error: 'Preencha todos os campos' });

    Try {
        Const user = await User.findById(req.userId);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Const ok = await bcrypt.compare(currentPassword, user.password);
        If (!ok) return res.status(401).json({ error: 'Senha atual incorreta' });

        User.password = await bcrypt.hash(newPassword, 10);
        Await user.save();

        Return res.json({ success: true, message: 'Senha alterada com sucesso' });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro interno' });
    }
});

app.put('/change-email', auth, async (req, res) => {
    Const { currentPassword, newEmail } = req.body;
    If (!currentPassword || !newEmail) return res.status(400).json({ error: 'Preencha todos os campos' });

    If (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(newEmail)) {
        Return res.status(400).json({ error: 'E-mail inválido' });
    }

    Try {
        Const user = await User.findById(req.userId);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Const ok = await bcrypt.compare(currentPassword, user.password);
        If (!ok) return res.status(401).json({ error: 'Senha atual incorreta' });

        Const cleanEmail = newEmail.trim().toLowerCase();
        Const exists = await User.findOne({ email: cleanEmail });
        If (exists && exists._id.toString() !== user._id.toString()) {
            Return res.status(400).json({ error: 'Este e-mail já está em uso' });
        }

        User.email = cleanEmail;
        Await user.save();

        Return res.json({
            Success: true,
            Message: 'E-mail alterado com sucesso',
            User: {
                Id: user._id,
                Name: user.name,
                Email: user.email,
                Avatar: user.avatar,
                Plan: user.plan || 'FREE',
                Studio: user.studio,
                RecoveryCode: user.recoveryCode,
                Profile: user.profile
            }
        });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro interno' });
    }
});

app.post('/recover-with-code', async (req, res) => {
    Const { email, recoveryCode, newPassword } = req.body;
    If (!email || !recoveryCode || !newPassword) return res.status(400).json({ error: 'Preencha todos os campos' });

    Try {
        Const cleanEmail = email.trim().toLowerCase();
        Const user = await User.findOne({ email: cleanEmail, recoveryCode });
        If (!user) return res.status(400).json({ error: 'Código inválido' });

        User.password = await bcrypt.hash(newPassword, 10);
        Await user.save();

        Return res.json({ success: true, message: 'Senha redefinida com sucesso' });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro interno' });
    }
});

// ================= GERENCIAMENTO DO PAINEL ADMIN / STUDIO =================

async function checkAdminPermission(userId) {
    Const admin = await User.findById(userId);
    Return admin && admin.studio === true;
}

Const handleGetAdminUsers = async (req, res) => {
    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) {
            Return res.status(403).json({ error: 'Acesso negado: Apenas o estúdio tem acesso ao gerenciamento de usuários.' });
        }

        Const users = await User.find().select('-password').sort({ createdAt: -1 });
        Return res.json({ success: true, count: users.length, users });
    } catch (err) {
        Console.error('[ADMIN LIST ERRO]:', err);
        Return res.status(500).json({ error: 'Erro ao listar usuários' });
    }
};

App.get('/api/admin/users', auth, handleGetAdminUsers);
app.get('/admin/users', auth, handleGetAdminUsers);

Const handleUpdateAdminUser = async (req, res) => {
    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) {
            Return res.status(403).json({ error: 'Acesso negado: Apenas administradores podem editar contas.' });
        }

        Const updateData = { ...req.body };
        Delete updateData.password;

        If (updateData.name && typeof updateData.name === 'string') {
            UpdateData.name = updateData.name.trim();
        }
        If (updateData.email && typeof updateData.email === 'string') {
            UpdateData.email = updateData.email.trim().toLowerCase();
        }
        If (updateData.plan && typeof updateData.plan === 'string') {
            UpdateData.plan = updateData.plan.toUpperCase();
        }
        If (updateData.studio !== undefined) {
            UpdateData.studio = Boolean(updateData.studio);
        }

        Const updatedUser = await User.findByIdAndUpdate(
            Req.params.id,
            { $set: updateData },
            { new: true, runValidators: true }
        ).select('-password');

        If (!updatedUser) {
            Return res.status(404).json({ error: 'Usuário não encontrado' });
        }

        Return res.json({
            Success: true,
            Message: 'Perfil de usuário atualizado no painel com sucesso!',
            User: updatedUser
        });
    } catch (err) {
        Console.error('[ADMIN EDIT USER ERRO]:', err);
        Return res.status(500).json({ error: 'Erro ao editar usuário via painel admin' });
    }
};

App.put('/api/admin/users/:id', auth, handleUpdateAdminUser);
app.put('/admin/user/:id', auth, handleUpdateAdminUser);

Const handleResetUserPassword = async (req, res) => {
    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) {
            Return res.status(403).json({ error: 'Acesso negado: Apenas administradores podem redefinir senhas.' });
        }

        Const { newPassword } = req.body;
        If (!newPassword || newPassword.trim().length < 4) {
            Return res.status(400).json({ error: 'Informe uma nova senha válida com no mínimo 4 caracteres.' });
        }

        Const user = await User.findById(req.params.id);
        If (!user) {
            Return res.status(404).json({ error: 'Usuário não encontrado' });
        }

        User.password = await bcrypt.hash(newPassword.trim(), 10);
        Await user.save();

        Return res.json({
            Success: true,
            Message: `Senha do usuário ${user.name || user.email} redefinida com sucesso!`
        });
    } catch (err) {
        Console.error('[ADMIN RESET PASSWORD ERRO]:', err);
        Return res.status(500).json({ error: 'Erro ao redefinir senha do usuário' });
    }
};

App.put('/api/admin/users/:id/reset-password', auth, handleResetUserPassword);
app.put('/admin/user/:id/reset-password', auth, handleResetUserPassword);

App.put('/admin/change-plan', auth, async (req, res) => {
    Const { email, plan } = req.body;
    If (!email || !plan) return res.status(400).json({ error: 'Preencha e-mail e plano' });

    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) return res.status(403).json({ error: 'Acesso negado' });

        Const cleanEmail = email.trim().toLowerCase();
        Const user = await User.findOneAndUpdate(
            { email: cleanEmail },
            { plan: plan.toUpperCase() },
            { new: true }
        ).select('-password');

        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({
            Success: true,
            Message: `Plano alterado para ${user.plan}`,
            User
        });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao alterar plano' });
    }
});

App.put('/admin/toggle-studio/:id', auth, async (req, res) => {
    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) return res.status(403).json({ error: 'Acesso negado' });

        Const user = await User.findById(req.params.id);
        If (!user) return res.status(404).json({ error: 'Usuário não encontrado' });

        User.studio = !user.studio;
        Await user.save();

        Return res.json({
            Success: true,
            Message: `Permissão de Studio ${user.studio ? 'ativada' : 'desativada'} com sucesso!`,
            Studio: user.studio
        });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao alterar permissão de estúdio' });
    }
});

App.delete('/admin/user/:id', auth, async (req, res) => {
    Try {
        Const isAdmin = await checkAdminPermission(req.userId);
        If (!isAdmin) return res.status(403).json({ error: 'Acesso negado' });

        Const deleted = await User.findByIdAndDelete(req.params.id);
        If (!deleted) return res.status(404).json({ error: 'Usuário não encontrado' });

        Return res.json({ success: true, message: 'Usuário removido com sucesso' });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao deletar usuário' });
    }
});

// ================= ROTAS DE SUPORTE =================
app.get('/support/messages/:targetUserId?', auth, async (req, res) => {
    Try {
        Const requestingUser = await User.findById(req.userId);
        If (!requestingUser) return res.status(404).json({ error: 'Usuário não encontrado' });

        Let queryUserId = req.userId;
        If (requestingUser.studio && req.params.targetUserId) {
            QueryUserId = req.params.targetUserId;
        }

        Const messages = await SupportMessage.find({ userId: queryUserId })
            .sort({ createdAt: 1 })
            .populate('senderId', 'name avatar email isOnline lastSeen');

        Return res.json({ success: true, messages });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao buscar mensagens' });
    }
});

App.get('/support/admin/chats', auth, async (req, res) => {
    Try {
        Const requestingUser = await User.findById(req.userId);
        If (!requestingUser || !requestingUser.studio) {
            Return res.status(403).json({ error: 'Acesso negado. Apenas estúdio autorizado.' });
        }

        Const chats = await SupportMessage.aggregate([
            { $sort: { createdAt: -1 } },             {$group: {
                    _id: "$userId",
                    lastMessage: { $first: "$message" },
                    lastMessageDate: { $first: "$createdAt" },
                    lastMessageStatus: { $first: "$status" },
                    lastMessageSenderModel: { $first: "$senderModel" },
                    unreadCount: {
                        $sum: {$cond: [
                                {
                                    $and: [
                                        { $eq: ["$senderModel", "user"] },
                                        { $ne: ["$status", "read"] }
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            { $sort: { lastMessageDate: -1 } }
        ]);

        Const populatedChats = await User.populate(chats, {
            Path: '_id',
            Select: 'name email avatar plan studio isOnline lastSeen'
        });

        Return res.json({ success: true, chats: populatedChats });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao listar chats' });
    }
});

App.post('/support/message', auth, async (req, res) => {
    Try {
        Const { message, targetUserId } = req.body;
        Const senderId = req.userId;

        Const sender = await User.findById(senderId);
        If (!sender) return res.status(404).json({ error: 'Usuário não encontrado' });

        Const isAdmin = sender.studio === true; 

        Const newMessage = await SupportMessage.create({
            UserId: isAdmin ? targetUserId : senderId,
            SenderId,
            SenderModel: isAdmin ? 'admin' : 'user',
            Message,
            Status: 'sent',
            CreatedAt: new Date()
        });

        Const populatedMessage = await SupportMessage.findById(newMessage._id)
            .populate('senderId', 'name avatar email isOnline lastSeen');

        If (isAdmin) {
            Io.to(`user_${targetUserId}`).emit('new_support_message', populatedMessage);
            Io.to('admin_support_room').emit('new_support_message', populatedMessage);
        } else {
            Io.to(`user_${senderId}`).emit('new_support_message', populatedMessage);
            Io.to('admin_support_room').emit('new_support_message', populatedMessage);
        }

        Return res.status(200).json({ success: true, message: populatedMessage });
    } catch (error) {
        Console.error(error);
        Return res.status(500).json({ error: error.message });
    }
});

App.post('/support/read', auth, async (req, res) => {
    Const { targetUserId } = req.body;
    Try {
        Const requestingUser = await User.findById(req.userId);
        If (!requestingUser) return res.status(404).json({ error: 'Usuário não encontrado' });

        Let chatUserId = req.userId;
        If (requestingUser.studio && targetUserId) {
            ChatUserId = targetUserId;
        }

        Const now = new Date();
        Await SupportMessage.updateMany(
            { userId: chatUserId, senderId: { $ne: req.userId }, status: {$ne: 'read' } },
            { $set: { status: 'read', readAt: now } }
        );

        Io.to(`user_${chatUserId}`).emit('messages_read', { userId: chatUserId, status: 'read', readAt: now });
        Io.to('admin_support_room').emit('messages_read', { userId: chatUserId, status: 'read', readAt: now });

        Return res.json({ success: true, readAt: now });
    } catch (err) {
        Console.error(err);
        Return res.status(500).json({ error: 'Erro ao marcar mensagens como lidas' });
    }
});

// ================= ROTAS DE REPORTES =================
app.get('/api/reports', async (req, res) => {
    Try {
        Const reports = await Report.find().sort({ createdAt: -1 });
        Return res.status(200).json({ success: true, reports });
    } catch (err) {
        Console.error('Erro ao listar reportes:', err);
        Return res.status(500).json({ error: 'Erro interno ao listar reportes' });
    }
});

App.post('/api/report', async (req, res) => {
    Try {
        Const { itemId, title, reason, userId } = req.body;
        Const newReport = await Report.create({ itemId, title, reason, userId });
        Io.to('admin_support_room').emit('new_report', newReport);

        Return res.status(200).json({ success: true, message: 'Reporte salvo com sucesso!', report: newReport });
    } catch (err) {
        Console.error('Erro ao processar reporte:', err);
        Return res.status(500).json({ error: 'Erro interno ao salvar reporte' });
    }
});

App.delete('/api/reports/:id', async (req, res) => {
    Try {
        Const { id } = req.params;
        If (!mongoose.Types.ObjectId.isValid(id)) {
            Return res.status(400).json({ error: 'ID de reporte inválido' });
        }

        Const deleted = await Report.findByIdAndDelete(id);
        If (!deleted) {
            Return res.status(404).json({ error: 'Reporte não encontrado' });
        }

        Return res.status(200).json({ success: true, message: 'Reporte deletado com sucesso!' });
    } catch (err) {
        Console.error('Erro ao deletar reporte:', err);
        Return res.status(500).json({ error: 'Erro interno ao deletar reporte' });
    }
});


// ================= NOVO: SISTEMA DE LIKE / DISLIKE =================

Const InteractionSchema = new mongoose.Schema({
    UserId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
    ItemId: { type: String, required: true },
    Type: { type: String, enum: ['LIKE', 'DISLIKE'], required: true },
    CreatedAt: { type: Date, default: Date.now }
});

// Garante que o mesmo usuário só pode ter um registro de voto por item
InteractionSchema.index({ userId: 1, itemId: 1 }, { unique: true });

Const Interaction = mongoose.model('Interaction', InteractionSchema);

// Registrar ou Alternar Voto (Like / Dislike)
app.post('/api/interaction/vote', auth, async (req, res) => {
    Try {
        Const { itemId, type } = req.body;
        Const userId = req.userId;

        If (!itemId || !['LIKE', 'DISLIKE'].includes(type)) {
            Return res.status(400).json({ error: 'Dados inválidos. Informe o itemId e type (LIKE ou DISLIKE).' });
        }

        // Verifica se já existe um voto deste usuário para este item
        Const existingVote = await Interaction.findOne({ userId, itemId });

        Let currentStatus = 'NEUTRAL';

        If (existingVote) {
            If (existingVote.type === type) {
                // Se clicou no mesmo botão novamente, remove o voto (torna neutro)
                Await Interaction.deleteOne({ _id: existingVote._id });
                CurrentStatus = 'NEUTRAL';
            } else {
                // Se o voto anterior era o oposto, atualiza para o novo tipo
                ExistingVote.type = type;
                Await existingVote.save();
                CurrentStatus = type;
            }
        } else {
            // Se não votou ainda, cria o novo registro
            Await Interaction.create({ userId, itemId, type });
            CurrentStatus = type;
        }

        // Calcula os totais atualizados para o item
        Const totalLikes = await Interaction.countDocuments({ itemId, type: 'LIKE' });
        Const totalDislikes = await Interaction.countDocuments({ itemId, type: 'DISLIKE' });

        Return res.status(200).json({
            Success: true,
            CurrentStatus,
            TotalLikes,
            TotalDislikes
        });

    } catch (err) {
        Console.error('Erro ao processar voto:', err);
        Return res.status(500).json({ error: 'Erro interno ao processar voto' });
    }
});

// Consultar o estado atual de votos de um item para o usuário logado
app.get('/api/interaction/:itemId', auth, async (req, res) => {
    Try {
        Const { itemId } = req.params;
        Const userId = req.userId;

        Const userVote = await Interaction.findOne({ userId, itemId });
        Const totalLikes = await Interaction.countDocuments({ itemId, type: 'LIKE' });
        Const totalDislikes = await Interaction.countDocuments({ itemId, type: 'DISLIKE' });

        Return res.status(200).json({
            Success: true,
            CurrentStatus: userVote ? userVote.type : 'NEUTRAL',
            TotalLikes,
            TotalDislikes
        });

    } catch (err) {
        Console.error('Erro ao buscar status de interação:', err);
        Return res.status(500).json({ error: 'Erro interno ao buscar interações' });
    }
});

// ================= FIM DAS ROTAS DE LIKE / DISLIKE =================


Server.listen(PORT, '0.0.0.0', () => {
    Console.log(`🚀 Servidor unificado do Prime Studio rodando na porta ${PORT}`);
});
