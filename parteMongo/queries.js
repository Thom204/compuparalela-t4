const dbName = "empresa_db";
const collectionName = "empresa_data";
const outputCollection = "emp_dept_join";
const dataDirectory = "Data2"; 

// Usar la base de datos
use(dbName);

db[collectionName].drop();
db[outputCollection].drop();


function loadJSONFiles() {
    const fs = require('fs');
    const path = require('path');
    
    try {
        if (!fs.existsSync(dataDirectory)) {
            print("Directorio no encontrado\n");
            return false;
        }
        
        const files = fs.readdirSync(dataDirectory);
        const jsonFiles = files.filter(file => file.endsWith('.json'));
        
        if (jsonFiles.length === 0) {
            print("No se encontraron archivos .json en el directorio.");
            return false;
        }
        
        print("Archivos encontrados: " + jsonFiles.length);
        
        let totalDocuments = 0;
        jsonFiles.forEach(function(file) {
            const filePath = path.join(dataDirectory, file);
            print("Cargando: " + file);
            
            const fileContent = fs.readFileSync(filePath, 'utf8');
            const data = JSON.parse(fileContent);
            
            if (Array.isArray(data)) {
                db[collectionName].insertMany(data);
                totalDocuments += data.length;
            } else {
                // Si es un solo objeto, insertarlo
                db[collectionName].insertOne(data);
                totalDocuments += 1;
            }
        });
        
        print(" Total de documentos cargados: " + totalDocuments);
        return true;
        
    } catch (error) {
        print("Error al cargar archivos: " + error.message);
        return false;
    }
}

const filesLoaded = loadJSONFiles();

print("Total de documentos en colección: " + db[collectionName].countDocuments());

const mapFunction = function() {
    // emite una dupla (key, {values})

    if (this.EMP && Array.isArray(this.EMP)) {
        this.EMP.forEach(function(emp) {
            emit(emp.dep, {
                type: "EMP",
                cedula: emp.cedula,
                nombre: emp.nombre,
                salario: emp.salario,
                dep: emp.dep
            });
        });
    }
    
    if (this.DEPT && Array.isArray(this.DEPT)) {
        this.DEPT.forEach(function(dept) {
            emit(dept.dep, {
                type: "DEPT",
                dep: dept.dep,
                nombredep: dept.nombredep,
                presupuesto: dept.presupuesto
            });
        });
    }
};

//nota importante, el reducer debe ser indempotente lo que significa que:
//reduce([A, B, C]) === reduce([reduce(key, [A, B]), C])
const reduceFunction = function(key, values) {

    // Acumuladores consistentes
    const empleados = {};
    let departamento = null;

    values.forEach(function(value) {

        // Caso 1: Entrada de mapa tipo EMP
        if (value.type === "EMP") {
            empleados[value.cedula] = {
                cedula: value.cedula,
                nombre: value.nombre,
                salario: value.salario
            };
        }

        // Caso 2: Entrada de mapa tipo DEPT
        else if (value.type === "DEPT") {
            // Si hay múltiples departamentos, todos deben coincidir.
            // Toma el primero válido; ignora duplicados idénticos.
            if (!departamento) {
                departamento = {
                    nombredep: value.nombredep,
                    presupuesto: value.presupuesto
                };
            }
        }

        // Caso 3: Entrada proveniente de otro reduce
        else {
            // Combinar empleados de reduce parcial
            if (value.empleados && Array.isArray(value.empleados)) {
                value.empleados.forEach(function(emp) {
                    empleados[emp.cedula] = emp;
                });
            }

            // Combinar departamento de reduce parcial
            if (value.departamento && !departamento) {
                departamento = value.departamento;
            }
        }
    });

    // Normalizar la salida final (IMPORTANTE PARA IDEMPOTENCIA)
    // Convertir hashmap → array ordenado
    const empleadosOrdenados = Object.values(empleados)
        .sort((a, b) => (a.cedula > b.cedula ? 1 : -1));

    return {
        dep: key,
        empleados: empleadosOrdenados,
        departamento: departamento
    };
};

//usar {out : {inline : 1}} esta deprecado y causa que mongosh se interrumpa antes de tiempo.
//mapReduce tambien esta deprecado pero no causa crashes si se maneja la output correctamente.

//usaremos outputCollection como salida y luego mostraremos el contenido.
const mr = db[collectionName].mapReduce(mapFunction, reduceFunction, {out: outputCollection});     

// por ultimo entregamos el join ordenado por dep.
db[outputCollection].find().pretty();