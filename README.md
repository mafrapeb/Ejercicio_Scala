# Ejercicio_Scala
Prueba Técnica

# App Scala

## Descripción
Este repositorio contiene un ejemplo de cómo crear una aplicación de Spark usando Scala y Maven. Se estructura el proyecto en clases con un enfoque orientado a objetos, y se aplican optimizaciones avanzadas como `Broadcast Join`, `Repartition` y `Cache` para mejorar el rendimiento.

### Estructura de Clases

1. **DataFrameFactory**: Encargada de la creación de los DataFrames `df1`, `df2`, y `df3`.
2. **DataFrameOperations**: Realiza las operaciones entre DataFrames como `leftJoin` e `innerJoin`.
3. **OptimizedProcess**: Implementa optimizaciones como `Broadcast Join`, reparticionamiento, y `Cache`.

### Requisitos

- **Scala** 2.12.15
- **Maven** 4.0.0
