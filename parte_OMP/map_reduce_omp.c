#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <time.h>

#define MAX_WORD_LENGTH 50 //Se define un tamaño maximo para la longitud del arreglo de palabras.
#define MAX_UNIQUE_WORDS 1000 //Se usa para definir el tamaño de los arreglos que van a guardar palabras unicas.

// Estructura para almacenar pares (palabra, conteo)
typedef struct {
    char word[MAX_WORD_LENGTH];
    int count;
} WordCount;

// Estructura para almacenar resultados de cada mapper
typedef struct {
    WordCount* pairs;
    int num_pairs;
    int mapper_id;
} MapperResult;

// Estructura para el controller
typedef struct {
    WordCount* pairs;
    int num_pairs;
    int reducer_id;  // A qué reducer va destinado
} ControllerData;

// Variables globales
char** global_array;
int array_size;
MapperResult* mapper_results;
ControllerData* controller_data;
WordCount* final_results;
int num_final_results;

// Función para buscar una palabra en un arreglo de WordCount
int find_word(WordCount* arr, int size, const char* word) {
    for (int i = 0; i < size; i++) {
        if (strcmp(arr[i].word, word) == 0) {
            return i;
        }
    }
    return -1;
}

// Función MAPPER - Procesa un fragmento del arreglo
void mapper(int start, int end, int mapper_id) {
    int thread_id = omp_get_thread_num();
    
    printf("\n=== MAPPER %d (Thread %d) ===\n", mapper_id, thread_id);
    printf("Fragmento asignado: ");
    for (int i = start; i < end; i++) {
        printf("%s ", global_array[i]);
    }
    printf("\n");
    
    // Arreglo temporal para contar palabras en este fragmento
    WordCount local_counts[MAX_UNIQUE_WORDS];
    int num_unique = 0;
    
    // Procesar cada palabra del fragmento
    for (int i = start; i < end; i++) {
        int idx = find_word(local_counts, num_unique, global_array[i]);
        
        if (idx == -1) {
            // Nueva palabra
            strcpy(local_counts[num_unique].word, global_array[i]);
            local_counts[num_unique].count = 1;
            num_unique++;
        } else {
            // Palabra ya existe, incrementar contador
            local_counts[idx].count++;
        }
    }
    
    // Guardar resultados del mapper
    mapper_results[mapper_id].pairs = (WordCount*)malloc(num_unique * sizeof(WordCount));
    mapper_results[mapper_id].num_pairs = num_unique;
    mapper_results[mapper_id].mapper_id = mapper_id;
    
    printf("Emite:\n");
    for (int i = 0; i < num_unique; i++) {
        mapper_results[mapper_id].pairs[i] = local_counts[i];
        printf("  (%s, %d)\n", local_counts[i].word, local_counts[i].count);
    }
}

// Función CONTROLLER - Agrupa y distribuye a los reducers (SECUENCIAL)
void controller(int n_mappers, int m_reducers) {
    printf("\n\n=== CONTROLLER (Secuencial) ===\n");
    
    // Paso 1: Consolidar todos los pares de los mappers
    WordCount all_pairs[MAX_UNIQUE_WORDS];
    int total_unique = 0;
    
    // Agrupar todas las palabras de todos los mappers
    for (int m = 0; m < n_mappers; m++) {
        for (int i = 0; i < mapper_results[m].num_pairs; i++) {
            char* word = mapper_results[m].pairs[i].word;
            int count = mapper_results[m].pairs[i].count;
            
            int idx = find_word(all_pairs, total_unique, word);
            
            if (idx == -1) {
                strcpy(all_pairs[total_unique].word, word);
                all_pairs[total_unique].count = count;
                total_unique++;
            } else {
                all_pairs[idx].count += count;
            }
        }
    }
    
    printf("\nParejas agrupadas:\n");
    for (int i = 0; i < total_unique; i++) {
        printf("  (%s, %d)\n", all_pairs[i].word, all_pairs[i].count);
    }
    
    // Paso 2: Distribuir entre los reducers
    controller_data = (ControllerData*)malloc(m_reducers * sizeof(ControllerData));
    
    for (int r = 0; r < m_reducers; r++) {
        controller_data[r].pairs = (WordCount*)malloc(MAX_UNIQUE_WORDS * sizeof(WordCount));
        controller_data[r].num_pairs = 0;
        controller_data[r].reducer_id = r;
    }
    
    printf("\nDistribución a Reducers:\n");
    for (int i = 0; i < total_unique; i++) {
        // Asignar cada palabra a un reducer (distribución round-robin)
        int reducer_id = i % m_reducers;
        
        int pos = controller_data[reducer_id].num_pairs;
        controller_data[reducer_id].pairs[pos] = all_pairs[i];
        controller_data[reducer_id].num_pairs++;
        
        printf("  (%s, %d) -> Reducer %d\n", 
               all_pairs[i].word, all_pairs[i].count, reducer_id);
    }
}

// Función REDUCER - Produce resultado final
void reducer(int reducer_id) {
    int thread_id = omp_get_thread_num();
    
    printf("\n=== REDUCER %d (Thread %d) ===\n", reducer_id, thread_id);
    printf("Recibió:\n");
    
    for (int i = 0; i < controller_data[reducer_id].num_pairs; i++) {
        printf("  (%s, %d)\n", 
               controller_data[reducer_id].pairs[i].word,
               controller_data[reducer_id].pairs[i].count);
    }
    
    printf("Emite (resultado final):\n");
    for (int i = 0; i < controller_data[reducer_id].num_pairs; i++) {
        printf("  (%s, %d)\n", 
               controller_data[reducer_id].pairs[i].word,
               controller_data[reducer_id].pairs[i].count);
    }
}

// Versión paralela con MapReduce
void mapreduce_parallel(int n_mappers, int m_reducers) {
    printf("\n********* VERSIÓN PARALELA CON MAPREDUCE *********\n");
    
    double start_time = omp_get_wtime();
    
    // Inicializar estructura para resultados de mappers
    mapper_results = (MapperResult*)malloc(n_mappers * sizeof(MapperResult));
    
    // FASE MAP (Paralela)
    #pragma omp parallel num_threads(n_mappers)
    {
        int mapper_id = omp_get_thread_num();
        int chunk_size = array_size / n_mappers;
        int start = mapper_id * chunk_size;
        int end = (mapper_id == n_mappers - 1) ? array_size : start + chunk_size;
        
        mapper(start, end, mapper_id);
    }
    
    // FASE CONTROLLER (Secuencial)
    controller(n_mappers, m_reducers);
    
    // FASE REDUCE (Paralela)
    #pragma omp parallel num_threads(m_reducers)
    {
        int reducer_id = omp_get_thread_num();
        reducer(reducer_id);
    }
    
    double end_time = omp_get_wtime();
    
    printf("\n\n=== RESULTADO FINAL ===\n");
    for (int r = 0; r < m_reducers; r++) {
        for (int i = 0; i < controller_data[r].num_pairs; i++) {
            printf("(%s, %d)\n", 
                   controller_data[r].pairs[i].word,
                   controller_data[r].pairs[i].count);
        }
    }
    
    printf("\nTiempo de ejecución paralela: %.6f segundos\n", end_time - start_time);
}

// Versión secuencial
void mapreduce_sequential() {
    printf("\n\n********* VERSIÓN SECUENCIAL *********\n");
    
    double start_time = omp_get_wtime();
    
    WordCount counts[MAX_UNIQUE_WORDS];
    int num_unique = 0;
    
    // Contar todas las palabras secuencialmente
    for (int i = 0; i < array_size; i++) {
        int idx = find_word(counts, num_unique, global_array[i]);
        
        if (idx == -1) {
            strcpy(counts[num_unique].word, global_array[i]);
            counts[num_unique].count = 1;
            num_unique++;
        } else {
            counts[idx].count++;
        }
    }
    
    double end_time = omp_get_wtime();
    
    printf("\nResultado final:\n");
    for (int i = 0; i < num_unique; i++) {
        printf("(%s, %d)\n", counts[i].word, counts[i].count);
    }
    
    printf("\nTiempo de ejecución secuencial: %.6f segundos\n", end_time - start_time);
}

int main() {
    // Variables configurables
    int n_mappers = 4;   // Número de mappers (2-16)
    int m_reducers = 2;  // Número de reducers (2-16)
    
    printf("Configuración:\n");
    printf("  n_mappers = %d\n", n_mappers);
    printf("  m_reducers = %d\n", m_reducers);
    
    // Crear arreglo de palabras con al menos 40 elementos
    const char* words[] = {
        "gato", "perro", "gato", "casa", "perro", "árbol", "gato", "casa",
        "perro", "sol", "luna", "gato", "perro", "casa", "árbol", "sol",
        "gato", "perro", "luna", "casa", "gato", "árbol", "perro", "sol",
        "casa", "gato", "perro", "luna", "árbol", "gato", "casa", "sol",
        "perro", "gato", "casa", "árbol", "perro", "sol", "gato", "luna",
        "casa", "perro", "gato", "árbol", "sol", "perro", "gato", "casa",
        "luna", "perro", "gato", "casa", "árbol", "sol", "gato", "perro"
    };
    
    array_size = sizeof(words) / sizeof(words[0]);
    
    // Copiar a arreglo global
    global_array = (char**)malloc(array_size * sizeof(char*));
    for (int i = 0; i < array_size; i++) {
        global_array[i] = (char*)malloc(MAX_WORD_LENGTH * sizeof(char));
        strcpy(global_array[i], words[i]);
    }
    
    printf("  Tamaño del arreglo = %d palabras\n", array_size);
    
    // Ejecutar versión paralela
    mapreduce_parallel(n_mappers, m_reducers);
    
    // Ejecutar versión secuencial
    mapreduce_sequential();
    
    // Liberar memoria
    free(global_array);
    free(mapper_results);
    free(controller_data);
    
    return 0;
}