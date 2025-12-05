#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

#define LONGITUD_MAX_PALABRA 50

// Estructura para almacenar pares (palabra, conteo)
typedef struct {
    char palabra[LONGITUD_MAX_PALABRA];
    int conteo;
} ParPalabraConteo;

// Estructura para almacenar resultados de cada mapper
typedef struct {
    ParPalabraConteo* pares;
    int num_pares;
    int id_mapper;
} ResultadoMapper;

// Estructura para el controller
typedef struct {
    ParPalabraConteo* pares;
    int num_pares;
    int id_reducer;
} DatosController;

// Variables globales
char** arreglo_palabras;          // Arreglo compartido de palabras
int tamano_arreglo;                // Tamaño del arreglo
ResultadoMapper* resultados_mappers;   // Resultados de todos los mappers
DatosController* datos_controller;     // Datos para cada reducer

// Función para buscar una palabra en un arreglo de pares
int buscar_palabra(ParPalabraConteo* arr, int tamano, const char* palabra) {
    for (int i = 0; i < tamano; i++) {
        if (strcmp(arr[i].palabra, palabra) == 0) {
            return i;
        }
    }
    return -1;
}

// Función MAPPER - Procesa un fragmento del arreglo
void mapper(int inicio, int fin, int id_mapper) {
    int id_hilo = omp_get_thread_num();
    
    printf("\n=== MAPPER %d (Hilo %d) ===\n", id_mapper, id_hilo);
    printf("Fragmento asignado: ");
    for (int i = inicio; i < fin; i++) {
        printf("%s ", arreglo_palabras[i]);
    }
    printf("\n");
    
    // Calcular tamaño del fragmento
    int tamano_fragmento = fin - inicio;
    
    // Crear arreglo para almacenar pares (puede tener duplicados)
    ParPalabraConteo* conteos_locales = (ParPalabraConteo*)malloc(tamano_fragmento * sizeof(ParPalabraConteo));
    int num_pares = 0;
    
    // Procesar cada palabra del fragmento
    for (int i = inicio; i < fin; i++) {
        int idx = buscar_palabra(conteos_locales, num_pares, arreglo_palabras[i]);
        
        if (idx == -1) {
            // Nueva palabra
            strcpy(conteos_locales[num_pares].palabra, arreglo_palabras[i]);
            conteos_locales[num_pares].conteo = 1;
            num_pares++;
        } else {
            // Palabra ya existe, incrementar contador
            conteos_locales[idx].conteo++;
        }
    }
    
    // Guardar resultados del mapper
    resultados_mappers[id_mapper].pares = conteos_locales;
    resultados_mappers[id_mapper].num_pares = num_pares;
    resultados_mappers[id_mapper].id_mapper = id_mapper;
    
    printf("Emite:\n");
    for (int i = 0; i < num_pares; i++) {
        printf("  (%s, %d)\n", conteos_locales[i].palabra, conteos_locales[i].conteo);
    }
}

// Función CONTROLLER - Agrupa y distribuye a los reducers (SECUENCIAL)
void controller(int n_mappers, int m_reducers) {
    printf("\n\n=== CONTROLLER (Secuencial) ===\n");
    
    // Calcular cuántos pares hay en total
    int total_pares = 0;
    for (int m = 0; m < n_mappers; m++) {
        total_pares += resultados_mappers[m].num_pares;
    }
    
    // Consolidar todos los pares de los mappers en un solo arreglo
    ParPalabraConteo* todos_pares = (ParPalabraConteo*)malloc(total_pares * sizeof(ParPalabraConteo));
    int indice_total = 0;
    
    // Copiar todos los pares
    for (int m = 0; m < n_mappers; m++) {
        for (int i = 0; i < resultados_mappers[m].num_pares; i++) {
            todos_pares[indice_total] = resultados_mappers[m].pares[i];
            indice_total++;
        }
    }
    
    // Agrupar palabras iguales
    ParPalabraConteo* pares_agrupados = (ParPalabraConteo*)malloc(total_pares * sizeof(ParPalabraConteo));
    int num_agrupados = 0;
    
    for (int i = 0; i < total_pares; i++) {
        int idx = buscar_palabra(pares_agrupados, num_agrupados, todos_pares[i].palabra);
        
        if (idx == -1) {
            pares_agrupados[num_agrupados] = todos_pares[i];
            num_agrupados++;
        } else {
            pares_agrupados[idx].conteo += todos_pares[i].conteo;
        }
    }
    
    printf("\nParejas agrupadas:\n");
    for (int i = 0; i < num_agrupados; i++) {
        printf("  (%s, %d)\n", pares_agrupados[i].palabra, pares_agrupados[i].conteo);
    }
    
    // Distribuir entre los reducers
    datos_controller = (DatosController*)malloc(m_reducers * sizeof(DatosController));
    
    for (int r = 0; r < m_reducers; r++) {
        datos_controller[r].pares = (ParPalabraConteo*)malloc(num_agrupados * sizeof(ParPalabraConteo));
        datos_controller[r].num_pares = 0;
        datos_controller[r].id_reducer = r;
    }
    
    printf("\nDistribución a Reducers:\n");
    for (int i = 0; i < num_agrupados; i++) {
        // Asignar cada palabra a un reducer (distribución round-robin)
        int id_reducer = i % m_reducers;
        
        int pos = datos_controller[id_reducer].num_pares;
        datos_controller[id_reducer].pares[pos] = pares_agrupados[i];
        datos_controller[id_reducer].num_pares++;
        
        printf("  (%s, %d) -> Reducer %d\n", 
               pares_agrupados[i].palabra, pares_agrupados[i].conteo, id_reducer);
    }
    
    free(todos_pares);
    free(pares_agrupados);
}

// Función REDUCER - Produce resultado final
void reducer(int id_reducer) {
    int id_hilo = omp_get_thread_num();
    
    printf("\n=== REDUCER %d (Hilo %d) ===\n", id_reducer, id_hilo);
    printf("Recibió:\n");
    
    for (int i = 0; i < datos_controller[id_reducer].num_pares; i++) {
        printf("  (%s, %d)\n", 
               datos_controller[id_reducer].pares[i].palabra,
               datos_controller[id_reducer].pares[i].conteo);
    }
    
    printf("Emite (resultado final):\n");
    for (int i = 0; i < datos_controller[id_reducer].num_pares; i++) {
        printf("  (%s, %d)\n", 
               datos_controller[id_reducer].pares[i].palabra,
               datos_controller[id_reducer].pares[i].conteo);
    }
}

// Versión paralela con MapReduce
void mapreduce_paralelo(int n_mappers, int m_reducers) {
    printf("\n********* VERSIÓN PARALELA CON MAPREDUCE *********\n");
    
    double tiempo_inicio = omp_get_wtime();
    
    // Inicializar estructura para resultados de mappers
    resultados_mappers = (ResultadoMapper*)malloc(n_mappers * sizeof(ResultadoMapper));
    
    // FASE MAP (Paralela)
    #pragma omp parallel num_threads(n_mappers)
    {
        int id_mapper = omp_get_thread_num();
        int tamano_chunk = tamano_arreglo / n_mappers;
        int inicio = id_mapper * tamano_chunk;
        int fin = (id_mapper == n_mappers - 1) ? tamano_arreglo : inicio + tamano_chunk;
        
        mapper(inicio, fin, id_mapper);
    }
    
    // FASE CONTROLLER (Secuencial)
    controller(n_mappers, m_reducers);
    
    // FASE REDUCE (Paralela)
    #pragma omp parallel num_threads(m_reducers)
    {
        int id_reducer = omp_get_thread_num();
        reducer(id_reducer);
    }
    
    double tiempo_fin = omp_get_wtime();
    
    printf("\n\n=== RESULTADO FINAL ===\n");
    for (int r = 0; r < m_reducers; r++) {
        for (int i = 0; i < datos_controller[r].num_pares; i++) {
            printf("(%s, %d)\n", 
                   datos_controller[r].pares[i].palabra,
                   datos_controller[r].pares[i].conteo);
        }
    }
    
    printf("\nTiempo de ejecución paralela: %.6f segundos\n", tiempo_fin - tiempo_inicio);
}

// Versión secuencial
void mapreduce_secuencial() {
    printf("\n\n********* VERSIÓN SECUENCIAL *********\n");
    
    double tiempo_inicio = omp_get_wtime();
    
    ParPalabraConteo* conteos = (ParPalabraConteo*)malloc(tamano_arreglo * sizeof(ParPalabraConteo));
    int num_unicos = 0;
    
    // Contar todas las palabras secuencialmente
    for (int i = 0; i < tamano_arreglo; i++) {
        int idx = buscar_palabra(conteos, num_unicos, arreglo_palabras[i]);
        
        if (idx == -1) {
            strcpy(conteos[num_unicos].palabra, arreglo_palabras[i]);
            conteos[num_unicos].conteo = 1;
            num_unicos++;
        } else {
            conteos[idx].conteo++;
        }
    }
    
    double tiempo_fin = omp_get_wtime();
    
    printf("\nResultado final:\n");
    for (int i = 0; i < num_unicos; i++) {
        printf("(%s, %d)\n", conteos[i].palabra, conteos[i].conteo);
    }
    
    printf("\nTiempo de ejecución secuencial: %.6f segundos\n", tiempo_fin - tiempo_inicio);
    
    free(conteos);
}

int main() {
    // Variables configurables (el monitor pondrá valores entre 2 y 16)
    int n_mappers = 4;   // Número de mappers
    int m_reducers = 2;  // Número de reducers
    
    printf("=== CONFIGURACIÓN ===\n");
    printf("n_mappers = %d\n", n_mappers);
    printf("m_reducers = %d\n", m_reducers);
    
    // Crear arreglo de palabras con al menos 40 elementos
    const char* palabras[] = {
        "gato", "perro", "gato", "casa", "perro", "arbol", "gato", "casa",
        "perro", "sol", "luna", "gato", "perro", "casa", "arbol", "sol",
        "gato", "perro", "luna", "casa", "gato", "arbol", "perro", "sol",
        "casa", "gato", "perro", "luna", "arbol", "gato", "casa", "sol",
        "perro", "gato", "casa", "arbol", "perro", "sol", "gato", "luna",
        "casa", "perro", "gato", "arbol", "sol", "perro", "gato", "casa",
        "luna", "perro", "gato", "casa", "arbol", "sol", "gato", "perro"
    };
    
    tamano_arreglo = sizeof(palabras) / sizeof(palabras[0]);
    
    // Copiar a arreglo global
    arreglo_palabras = (char**)malloc(tamano_arreglo * sizeof(char*));
    for (int i = 0; i < tamano_arreglo; i++) {
        arreglo_palabras[i] = (char*)malloc(LONGITUD_MAX_PALABRA * sizeof(char));
        strcpy(arreglo_palabras[i], palabras[i]);
    }
    
    printf("Tamaño del arreglo = %d palabras\n", tamano_arreglo);
    
    // Ejecutar versión paralela
    mapreduce_paralelo(n_mappers, m_reducers);
    
    // Ejecutar versión secuencial
    mapreduce_secuencial();
    
    // Liberar memoria
    for (int i = 0; i < tamano_arreglo; i++) {
        free(arreglo_palabras[i]);
    }
    free(arreglo_palabras);
    free(resultados_mappers);
    free(datos_controller);
    
    return 0;
}