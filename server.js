const express = require('express');
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const multer = require('multer');

const {
    S3Client,
    PutObjectCommand
} = require('@aws-sdk/client-s3');

const cloudinary = require('cloudinary').v2;
const streamifier = require('streamifier');

require('dotenv').config();


const app = express();


app.use(cors());
app.use(express.json());


// ================= STORAGE =================

const STORAGE = process.env.STORAGE || "r2";



// ================= MONGO =================

mongoose.connect(process.env.MONGO_URL)

.then(()=>{

    console.log('MongoDB conectado');

})

.catch(err=>{

    console.error(err);

});




// ================= USER =================

const UserSchema = new mongoose.Schema({

    email:{
        type:String,
        unique:true
    },


    password:String,


    name:String,


    avatar:String,


    recoveryCode:{
        type:String,
        unique:true
    },


    profile:{
        type:mongoose.Schema.Types.Mixed,
        default:{}
    },


    createdAt:{
        type:Date,
        default:Date.now
    }


});


const User = mongoose.model(
    'User',
    UserSchema
);




// ================= FUNÇÕES =================

function generateRecoveryCode(){

    const a =
    Math.floor(
        1000 + Math.random()*9000
    );


    const b =
    Math.floor(
        1000 + Math.random()*9000
    );


    return `SG-${a}-${b}`;

}



async function createUniqueRecoveryCode(){

    let code;


    do{

        code = generateRecoveryCode();


    }while(
        await User.findOne({
            recoveryCode:code
        })
    );


    return code;

}




// ================= AUTH =================

function auth(req,res,next){


    const token =
    req.headers.authorization
    ?.replace('Bearer ','');


    if(!token){

        return res.status(401).json({

            error:'Token ausente'

        });

    }



    try{


        const decoded =
        jwt.verify(
            token,
            process.env.JWT_SECRET
        );


        req.userId =
        decoded.id;


        next();



    }catch{


        return res.status(401).json({

            error:'Token inválido'

        });


    }


}




// ================= R2 =================


const r2 = new S3Client({

    region:'auto',


    endpoint:
    `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,


    credentials:{

        accessKeyId:
        process.env.R2_ACCESS_KEY_ID,


        secretAccessKey:
        process.env.R2_SECRET_ACCESS_KEY

    }

});




// ================= CLOUDINARY =================


cloudinary.config({

    cloud_name:
    process.env.CLOUDINARY_CLOUD_NAME,


    api_key:
    process.env.CLOUDINARY_API_KEY,


    api_secret:
    process.env.CLOUDINARY_API_SECRET

});




// ================= MULTER =================


const upload =
multer({

    storage:
    multer.memoryStorage()

});

// ================= UPLOAD AVATAR =================

app.post(
'/upload-avatar',
upload.single('file'),
async(req,res)=>{


    try{


        if(!req.file){

            return res.status(400).json({

                error:'Arquivo não enviado'

            });

        }



        // ==========================
        // CLOUDFLARE R2
        // ==========================

        if(STORAGE === "r2"){


            const ext =
            req.file.originalname
            .split('.')
            .pop();



            const fileName =
            `avatars/${Date.now()}-${Math.random()
            .toString(36)
            .substring(2)}.${ext}`;



            await r2.send(

                new PutObjectCommand({

                    Bucket:
                    process.env.R2_BUCKET,


                    Key:
                    fileName,


                    Body:
                    req.file.buffer,


                    ContentType:
                    req.file.mimetype,


                    CacheControl:
                    'public, max-age=31536000'

                })

            );



            return res.json({

                success:true,


                url:
                `${process.env.R2_PUBLIC_URL}/${fileName}`

            });


        }





        // ==========================
        // CLOUDINARY
        // ==========================


        if(STORAGE === "cloudinary"){



            const result =
            await new Promise(
            (resolve,reject)=>{


                const stream =
                cloudinary
                .uploader
                .upload_stream(

                {

                    folder:"avatars"

                },


                (error,result)=>{


                    if(error){

                        reject(error);

                    }else{

                        resolve(result);

                    }


                });


                streamifier
                .createReadStream(
                    req.file.buffer
                )
                .pipe(stream);



            });



            return res.json({

                success:true,


                url:
                result.secure_url

            });


        }





        return res.status(500).json({

            error:
            "Storage não configurado"

        });



    }catch(err){


        console.error(
            "UPLOAD ERROR:",
            err
        );


        return res.status(500).json({

            error:
            "Erro no upload"

        });


    }


});




// ================= REGISTER =================


app.post(
'/register',
async(req,res)=>{


const {

    email,

    password,

    name,

    avatar,

    profile={}

}=req.body;



if(!email || !password || !name){


return res.status(400).json({

    error:
    'Preencha todos os campos'

});


}



try{


const exists =
await User.findOne({
    email
});



if(exists){


return res.status(400).json({

    error:
    'Email já cadastrado'

});


}




const hash =
await bcrypt.hash(
    password,
    10
);



const recoveryCode =
await createUniqueRecoveryCode();




const user =
await User.create({

    email,

    password:hash,

    name,

    avatar,

    recoveryCode,

    profile

});




return res.json({

    success:true,


    recoveryCode,


    user:{

        id:user._id,

        name:user.name,

        email:user.email,

        avatar:user.avatar,

        profile:user.profile

    }


});



}catch(err){


console.error(err);


return res.status(500).json({

    error:
    'Erro interno'

});


}



});






// ================= LOGIN =================


app.post(
'/login',
async(req,res)=>{


const {

email,

password

}=req.body;




try{


const user =
await User.findOne({
    email
});



if(!user){


return res.status(400).json({

    error:
    'Usuário não encontrado'

});


}



const ok =
await bcrypt.compare(
    password,
    user.password
);



if(!ok){


return res.status(401).json({

    error:
    'Senha inválida'

});


}




const token =
jwt.sign(

{

id:user._id

},

process.env.JWT_SECRET,

{

expiresIn:'30d'

}

);





return res.json({

token,


user:{


id:user._id,


name:user.name,


email:user.email,


avatar:user.avatar,


profile:user.profile


}



});




}catch(err){


console.error(err);


return res.status(500).json({

    error:
    'Erro interno'

});


}


});





// ================= ME =================


app.get(
'/me',
auth,
async(req,res)=>{


try{


const user =
await User.findById(
    req.userId
)
.select('-password');



if(!user){


return res.status(404).json({

    error:
    'Usuário não encontrado'

});


}



return res.json({

user:{

id:user._id,

name:user.name,

email:user.email,

avatar:user.avatar,

recoveryCode:user.recoveryCode,

profile:user.profile

}

});



}catch(err){


console.error(err);


return res.status(500).json({

    error:
    'Erro interno'

});


}


});


// ================= UPDATE PROFILE =================

app.put(
'/profile',
auth,
async(req,res)=>{


const {

name,

avatar,

profile={}

}=req.body;



try{


const user =
await User.findById(
    req.userId
);



if(!user){


return res.status(404).json({

    error:
    'Usuário não encontrado'

});


}




if(typeof name === 'string'){

    user.name =
    name.trim();

}




if(typeof avatar === 'string' && avatar.trim()){

    user.avatar =
    avatar;

}




user.profile = {

    ...user.profile,

    ...profile

};



await user.save();




return res.json({

success:true,


user:{

id:user._id,

name:user.name,

email:user.email,

avatar:user.avatar,

recoveryCode:user.recoveryCode,

profile:user.profile

}


});




}catch(err){


console.error(err);


return res.status(500).json({

error:
'Erro interno'

});


}


});






// ================= VERIFY CURRENT PASSWORD =================

app.post(
'/verify-password',
auth,
async(req,res)=>{


const {
    currentPassword
}=req.body;



if(!currentPassword){

return res.status(400).json({

    error:
    'Senha não informada'

});

}



try{


const user =
await User.findById(
    req.userId
);



if(!user){


return res.status(404).json({

    error:
    'Usuário não encontrado'

});


}



const valid =
await bcrypt.compare(

currentPassword,

user.password

);



return res.json({

    valid

});



}catch(err){


console.error(err);


return res.status(500).json({

    error:
    'Erro interno'

});


}


});


// ================= CHANGE PASSWORD =================


app.put(
'/change-password',
auth,
async(req,res)=>{


const {

currentPassword,

newPassword

}=req.body;




if(!currentPassword || !newPassword){


return res.status(400).json({

error:
'Preencha todos os campos'

});


}



try{


const user =
await User.findById(
    req.userId
);



if(!user){


return res.status(404).json({

error:
'Usuário não encontrado'

});


}




const ok =
await bcrypt.compare(

currentPassword,

user.password

);




if(!ok){


return res.status(401).json({

error:
'Senha atual incorreta'

});


}





user.password =
await bcrypt.hash(
    newPassword,
    10
);




await user.save();




return res.json({

success:true,

message:
'Senha alterada com sucesso'

});




}catch(err){


console.error(err);


return res.status(500).json({

error:
'Erro interno'

});


}


});





// ================= CHANGE EMAIL =================

app.put(
'/change-email',
auth,
async(req,res)=>{

const {

currentPassword,

newEmail

}=req.body;


if(!currentPassword || !newEmail){

return res.status(400).json({

error:'Preencha todos os campos'

});

}


const emailValid =
/^[^\s@]+@[^\s@]+\.[^\s@]+$/
.test(newEmail);


if(!emailValid){

return res.status(400).json({

error:'E-mail inválido'

});

}

try{


const user =
await User.findById(
req.userId
);


if(!user){

return res.status(404).json({

error:'Usuário não encontrado'

});

}


const ok =
await bcrypt.compare(

currentPassword,

user.password

);


if(!ok){

return res.status(401).json({

error:'Senha atual incorreta'

});

}


const exists =
await User.findOne({

email:newEmail

});


if(exists && exists._id.toString() !== user._id.toString()){

return res.status(400).json({

error:'Este e-mail já está em uso'

});

}


user.email = newEmail.trim();

await user.save();


return res.json({

success:true,

message:'E-mail alterado com sucesso',

user:{

id:user._id,

name:user.name,

email:user.email,

avatar:user.avatar,

recoveryCode:user.recoveryCode,

profile:user.profile

}

});


}catch(err){

console.error(err);

return res.status(500).json({

error:'Erro interno'

});

}

});






// ================= RECOVER WITH CODE =================


app.post(
'/recover-with-code',
async(req,res)=>{


const {

email,

recoveryCode,

newPassword

}=req.body;




if(!email || !recoveryCode || !newPassword){


return res.status(400).json({

error:
'Preencha todos os campos'

});


}




try{


const user =
await User.findOne({

email,

recoveryCode

});




if(!user){


return res.status(400).json({

error:
'Código inválido'

});


}




user.password =
await bcrypt.hash(

newPassword,

10

);




await user.save();




return res.json({

success:true,

message:
'Senha redefinida com sucesso'

});




}catch(err){


console.error(err);


return res.status(500).json({

error:
'Erro interno'

});


}


});








// ================= START =================


const PORT =
process.env.PORT || 3000;



app.listen(
PORT,
()=>{


console.log(
'Servidor rodando na porta',
PORT
);


});