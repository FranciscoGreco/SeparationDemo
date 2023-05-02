using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Unity.Assertions;
using Unity.Burst;
using Unity.Burst.Intrinsics;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;
using Unity.Transforms;
using UnityEngine;
using Random = Unity.Mathematics.Random;

namespace DefaultNamespace
{
    [UpdateInGroup(typeof(SimulationSystemGroup))]
    public partial class GameSimulationGroup : ComponentSystemGroup
    {
    }
    
    [UpdateInGroup(typeof(SimulationSystemGroup))]
    [UpdateAfter(typeof(GameSimulationGroup))]
    public partial class EndTransactionGroup : ComponentSystemGroup
    {
        protected override void OnUpdate()
        {
            // EntityManager.ExclusiveEntityTransactionDependency.Complete(); //Remove if you want simulation to run completely separate from rendering
            if (!EntityManager.ExclusiveEntityTransactionDependency.IsCompleted) 
                return;
            
            EntityManager.EndExclusiveEntityTransaction();
            EntityManager.ExclusiveEntityTransactionDependency.Complete();
            
            base.OnUpdate();
        }
    }
    
    [UpdateInGroup(typeof(GameSimulationGroup), OrderFirst = true)]
    public partial class BeginTransaction : SystemBase
    {
        protected override void OnCreate()
        {
            EntityManager.AddComponent<EntityTransactionSingleton>(SystemHandle);
        } 
    
        protected override void OnUpdate()
        {
            var entityTransaction = EntityManager.BeginExclusiveEntityTransaction();
            var transactionSingleton = SystemAPI.GetSingletonRW<EntityTransactionSingleton>();
            transactionSingleton.ValueRW.EntityManager = entityTransaction.EntityManager;
        }
    }

    [UpdateInGroup(typeof(GameSimulationGroup))]
    public partial class CreateEntities : SystemBase
    {
        public const int EntityCount = 100000;
        public const float ChangePercentage = 0.005f;
        private Entity prefab;
        
        protected override void OnCreate()
        {
            prefab = EntityManager.CreateEntity(typeof(SimulationComponent), typeof(SimulationComponent2), typeof(SimulationComponent3), typeof(SimulationComponent4), typeof(SimulationComponent5), typeof(LinkedEntityGroup), typeof(SimulationBuffer1), typeof(Prefab));
            var child = EntityManager.CreateEntity(typeof(SimulationComponent4), typeof(SimulationComponent5), typeof(Parent), typeof(Prefab));
            var linkedGroup = EntityManager.GetBuffer<LinkedEntityGroup>(prefab).Reinterpret<Entity>();
            linkedGroup.Add(prefab);
            linkedGroup.Add(child);
            var simulationBuffer = EntityManager.GetBuffer<SimulationBuffer1>(prefab);
            simulationBuffer.Add(new SimulationBuffer1 { SomeValue = 10, Entity1 = prefab, Entity2 = child });
            simulationBuffer.Add(new SimulationBuffer1 { SomeValue = 20, Entity1 = prefab, Entity2 = child });
            simulationBuffer.Add(new SimulationBuffer1 { SomeValue = 30, Entity1 = prefab, Entity2 = child });

            EntityManager.SetComponentData(prefab, new SimulationComponent2 { SomeValue = float4x4.identity });
            EntityManager.SetComponentData(prefab, new SimulationComponent3 { SomeValue = float4x4.identity * 2 });
            EntityManager.SetComponentData(prefab, new SimulationComponent4 { SomeValue = 80 });
            EntityManager.SetComponentData(child, new Parent { Value = prefab });
            EntityManager.Instantiate(prefab, EntityCount, Allocator.Temp);
        } 

        protected override void OnUpdate()
        {
            var transactionSingleton = SystemAPI.GetSingletonRW<EntityTransactionSingleton>();

            Dependency = new CreateEntitiesInJob
            {
                EntityManager = transactionSingleton.ValueRW.EntityManager,
                ToClone = prefab
            }.Schedule(Dependency);

            var manager = EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, Dependency);
        }
    }

    [BurstCompile]
    public struct CreateEntitiesInJob : IJob
    {
        public Entity ToClone;
        public EntityManager EntityManager;
        
        public void Execute()
        {
            EntityManager.Instantiate(ToClone, (int)(CreateEntities.EntityCount * CreateEntities.ChangePercentage), Allocator.Temp);
        }
    }
    
    public struct StallJob : IJob
    {
        public void Execute()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            while (stopwatch.ElapsedMilliseconds < 100)
            {
            }
        }
    }
    
    [UpdateAfter(typeof(CreateEntities))]
    [UpdateInGroup(typeof(GameSimulationGroup))]
    public partial struct ProcessEntitiesISystem : ISystem
    {
        public void OnUpdate(ref SystemState state)
        {
            var transactionSingleton = SystemAPI.GetSingletonRW<EntityTransactionSingleton>();

            new DummyProcessJob().ScheduleParallel();
            
            state.Dependency = new StallJob().Schedule(state.Dependency);

            var manager = state.EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, state.Dependency);
        }
    }

    [BurstCompile]
    public partial struct DummyProcessJob : IJobEntity
    {
        public void Execute(ref SimulationComponent dummyComponent)
        {
            dummyComponent.SomeValue += 400;
        }
    }
    
    [UpdateAfter(typeof(ProcessEntitiesISystem))]
    [UpdateInGroup(typeof(GameSimulationGroup))]
    public partial class DeleteEntities : SystemBase
    {
        protected override void OnUpdate()
        {
            var entityManager = SystemAPI.GetSingletonRW<EntityTransactionSingleton>().ValueRW.EntityManager;
            var random = new NativeReference<Random>(new Random(10), WorldUpdateAllocator);
            var toDestroy = new NativeList<Entity>(1000, WorldUpdateAllocator);
            
            Dependency = new GatherEntitiesToDeleteJob
            {
                Random = random,
                ToDestroy = toDestroy
            }.Schedule(Dependency);

            Dependency = new DestroyEntitiesJob
            {
                EntityManager = entityManager,
                ToDestroy = toDestroy
            }.Schedule(Dependency);
            
            var manager = entityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, Dependency);
        }
    }

    public struct DestroyEntitiesJob : IJob
    {
        public NativeList<Entity> ToDestroy;
        public EntityManager EntityManager;
        
        public void Execute()
        {
            EntityManager.DestroyEntity(ToDestroy.AsArray());
        }
    }
    
    [BurstCompile]
    [WithAll(typeof(LinkedEntityGroup))]
    public partial struct GatherEntitiesToDeleteJob : IJobEntity
    {
        public NativeReference<Random> Random;
        public NativeList<Entity> ToDestroy;

        public void Execute(Entity entity)
        {
            var randomValue = Random.Value;
            if (randomValue.NextFloat(1) < CreateEntities.ChangePercentage) 
                ToDestroy.Add(entity);
            Random.Value = randomValue;
        }
    }

    [UpdateInGroup(typeof(EndTransactionGroup))]
    [BurstCompile]
    public partial struct EndTransaction : ISystem
    {
        private EntityManager _renderingWorldEntities;
        private NativeList<int> _previousState;
        private NativeList<EntityRemapUtility.EntityRemapInfo> _entityRemap;
        private NativeList<ComponentType> _componentsToCopy;
        private NativeList<ComponentType> _buffersToCopy;
        private NativeList<ComponentType> _allTypesToCopy;
        private EntityQuery _nonPrefabQuery;
        private EntityQuery _nonPrefabQueryRenderingWorld;
        private EntityArchetype _emptyArchetype;
        private DynamicComponentHandles _componentTypeHandles;
        private DynamicComponentHandles _targetComponentTypeHandles;        
        private DynamicComponentHandles _bufferTypeHandles;
        private DynamicComponentHandles _targetBufferTypeHandles;

        public void OnCreate(ref SystemState state)
        {
            foreach (var world in World.All)
            {
                if (world.Name == "Rendering World")
                    _renderingWorldEntities = world.EntityManager;
            }

            _previousState = new NativeList<int>(Allocator.Persistent);
            _entityRemap = new NativeList<EntityRemapUtility.EntityRemapInfo>(Allocator.Persistent);

            _componentsToCopy = new NativeList<ComponentType>(7, Allocator.Persistent);
            _componentsToCopy.Add(typeof(SimulationComponent));
            _componentsToCopy.Add(typeof(SimulationComponent2));
            _componentsToCopy.Add(typeof(SimulationComponent3));
            _componentsToCopy.Add(typeof(SimulationComponent4));
            _componentsToCopy.Add(typeof(SimulationComponent5));
            _componentsToCopy.Add(typeof(Parent));
            
            _buffersToCopy = new NativeList<ComponentType>(7, Allocator.Persistent);
            _buffersToCopy.Add(typeof(SimulationBuffer1));

            _allTypesToCopy = new NativeList<ComponentType>(Allocator.Persistent);
            _allTypesToCopy.AddRange(_componentsToCopy.AsArray());
            _allTypesToCopy.AddRange(_buffersToCopy.AsArray());

            var queryBuilder = new EntityQueryBuilder(Allocator.Temp).WithOptions(EntityQueryOptions.IncludeDisabledEntities | EntityQueryOptions.IgnoreComponentEnabledState);
            _nonPrefabQuery = state.GetEntityQuery(queryBuilder);
            _nonPrefabQueryRenderingWorld = _renderingWorldEntities.CreateEntityQuery(queryBuilder);
            _emptyArchetype = _renderingWorldEntities.CreateArchetype();
        }

        private void UpdateDynamicTypeHandles(EntityManager entityManager)
        {
            _componentTypeHandles = new DynamicComponentHandles(_componentsToCopy.Length);
            for (int i = 0; i < _componentsToCopy.Length; i++)
                _componentTypeHandles[i] = entityManager.GetDynamicComponentTypeHandle(_componentsToCopy[i]);

            _targetComponentTypeHandles = new DynamicComponentHandles(_componentsToCopy.Length);
            for (int i = 0; i < _componentsToCopy.Length; i++)
                _targetComponentTypeHandles[i] = _renderingWorldEntities.GetDynamicComponentTypeHandle(_componentsToCopy[i]);
            
            _bufferTypeHandles = new DynamicComponentHandles(_buffersToCopy.Length);
            for (int i = 0; i < _buffersToCopy.Length; i++)
                _bufferTypeHandles[i] = entityManager.GetDynamicComponentTypeHandle(_buffersToCopy[i]);

            _targetBufferTypeHandles = new DynamicComponentHandles(_buffersToCopy.Length);
            for (int i = 0; i < _buffersToCopy.Length; i++)
                _targetBufferTypeHandles[i] = _renderingWorldEntities.GetDynamicComponentTypeHandle(_buffersToCopy[i]);
        }
        
        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var entityManager = state.EntityManager;
            using var createdEntities = new NativeList<Entity>(Allocator.TempJob);
            using var destroyedEntities = new NativeList<Entity>(Allocator.TempJob);
                
            entityManager.GetCreatedAndDestroyedEntities(_previousState, createdEntities, destroyedEntities);
                
            for (int i = createdEntities.Length - 1; i >= 0; i--)
            {
                if (!_nonPrefabQuery.MatchesIgnoreFilter(createdEntities[i]))
                    createdEntities.RemoveAtSwapBack(i);
            }
                
            _entityRemap.Resize(entityManager.EntityCapacity + 1, NativeArrayOptions.ClearMemory);
            var entityRemapArray = _entityRemap.AsArray();

            var newEntities = _renderingWorldEntities.CreateEntity(_emptyArchetype, createdEntities.Length, Allocator.Temp);
            for (var i = 0; i < createdEntities.Length; i++)
                EntityRemapUtility.AddEntityRemapping(ref entityRemapArray, createdEntities[i], newEntities[i]);

            var destroyedEntitiesRemap = new NativeList<Entity>(destroyedEntities.Length, Allocator.Temp);
            foreach (var entity in destroyedEntities)
                destroyedEntitiesRemap.Add(RemapUtils.RemapEntity(ref entityRemapArray, entity));
            _renderingWorldEntities.DestroyEntity(destroyedEntitiesRemap.AsArray());
            
            UpdateDynamicTypeHandles(entityManager);

            new SynchronizeStructuralChangesJob
            {
                AllTypesToCopy = _allTypesToCopy.AsArray(),
                RenderingWorldEntities = _renderingWorldEntities,
                EntityTypeHandle = SystemAPI.GetEntityTypeHandle(),
                EntityRemapArray = entityRemapArray,
                LastSystemVersion = state.LastSystemVersion,
                AddedTypes = CollectionHelper.CreateNativeArray<TypeIndex>(100, state.WorldUpdateAllocator),
                RemovedTypes = CollectionHelper.CreateNativeArray<TypeIndex>(100, state.WorldUpdateAllocator),
                ComponentsToAdd = new NativeList<ComponentType>(100, state.WorldUpdateAllocator),
                NewArchetypeComponents = new NativeList<ComponentType>(100, state.WorldUpdateAllocator),
            }.Run(_nonPrefabQuery);

            UpdateDynamicTypeHandles(entityManager);

            var synchronizeDataJob = new SynchronizeDataJob
            {
                ComponentTypeHandles = _componentTypeHandles,
                TargetComponentTypeHandles = _targetComponentTypeHandles,
                BufferTypeHandles = _bufferTypeHandles,
                TargetBufferTypeHandles = _targetBufferTypeHandles,
                EntityRemap = entityRemapArray,
                EntityTypeHandle = SystemAPI.GetEntityTypeHandle(),
                RenderingWorldEntities = _renderingWorldEntities,
                ComponentsToCopyTypes = _componentsToCopy.AsArray(),
                BuffersToCopyTypes = _buffersToCopy.AsArray(),
                LastSystemVersion = state.LastSystemVersion
            };
            state.Dependency = synchronizeDataJob.ScheduleParallelByRef(_nonPrefabQuery, state.Dependency);
            state.Dependency.Complete();
            
            // Assert.AreEqual(_nonPrefabQueryRenderingWorld.CalculateEntityCount(), _nonPrefabQuery.CalculateEntityCount());
            //
            // foreach (var (parent, entity) in SystemAPI.Query<Parent>().WithEntityAccess())
            // {
            //     var otherEntity = RemapUtils.RemapEntity(ref entityRemapArray, entity);
            //     var otherParent = _renderingWorldEntities.GetComponentData<Parent>(otherEntity).Value;
            //     Assert.IsTrue(entityManager.Exists(parent.Value));
            //     Assert.AreEqual(RemapUtils.RemapEntity(ref entityRemapArray, parent.Value), otherParent);
            // }
            //
            // foreach (var (data, entity) in SystemAPI.Query<SimulationComponent>().WithEntityAccess())
            // {
            //     var otherEntity = RemapUtils.RemapEntity(ref entityRemapArray, entity);
            //     var otherData = _renderingWorldEntities.GetComponentData<SimulationComponent>(otherEntity).SomeValue;
            //     Assert.AreEqual(otherData, data.SomeValue);
            // }
            //
            // foreach (var (data, entity) in SystemAPI.Query<DynamicBuffer<SimulationBuffer1>>().WithEntityAccess())
            // {
            //     var otherEntity = RemapUtils.RemapEntity(ref entityRemapArray, entity);
            //     var otherData = _renderingWorldEntities.GetBuffer<SimulationBuffer1>(otherEntity);
            //     Assert.AreEqual(data.Length, otherData.Length);
            //
            //     for (int i = 0; i < data.Length; i++)
            //     {
            //         Assert.AreEqual(data[i].SomeValue, otherData[i].SomeValue);
            //         Assert.AreEqual(data[i].Entity1, RemapUtils.RemapEntity(ref entityRemapArray, otherData[i].Entity1));
            //         Assert.AreEqual(data[i].Entity2, RemapUtils.RemapEntity(ref entityRemapArray, otherData[i].Entity2));
            //     }
            // }
        }
    }

    [BurstCompile]
    public struct SynchronizeStructuralChangesJob : IJobChunk
    {
        public EntityTypeHandle EntityTypeHandle;
        public EntityManager RenderingWorldEntities;
        [ReadOnly] public NativeArray<EntityRemapUtility.EntityRemapInfo> EntityRemapArray;
        [ReadOnly] public NativeArray<ComponentType> AllTypesToCopy;
        public uint LastSystemVersion;

        public NativeArray<TypeIndex> AddedTypes;
        public NativeArray<TypeIndex> RemovedTypes;
        public NativeList<ComponentType> NewArchetypeComponents;
        public NativeList<ComponentType> ComponentsToAdd;

        public unsafe void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
        {
            if (!chunk.DidOrderChange(LastSystemVersion))
                return;
            
            var archetypeComponents = chunk.Archetype.GetComponentTypes();
            ComponentsToAdd.Clear();
            
            foreach (var componentToCopy in AllTypesToCopy)
            {
                if (ContainsComponent(archetypeComponents, componentToCopy))
                    ComponentsToAdd.Add(componentToCopy);
            }

            bool ContainsComponent(NativeArray<ComponentType> componentTypes, ComponentType component)
            {
                foreach (var componentType in componentTypes)
                    if (componentType.TypeIndex == component.TypeIndex)
                        return true;

                return false;
            }

            var chunkEntities = chunk.GetNativeArray(EntityTypeHandle);
            foreach (var entity in chunkEntities)
            {
                var remappedEntity = RemapUtils.RemapEntity(ref EntityRemapArray, entity);
                var renderingEntityArchetype = RenderingWorldEntities.GetStorageInfo(remappedEntity).Chunk.Archetype;
                EntityArchetype.CalculateDifference(chunk.Archetype, renderingEntityArchetype, (TypeIndex*)AddedTypes.GetUnsafePtr(), out var addedTypesCount, (TypeIndex*)RemovedTypes.GetUnsafePtr(), out var removedTypesCount);
                
                if (addedTypesCount == 0 && removedTypesCount == 0)
                    continue;
                
                NewArchetypeComponents.AddRange(ComponentsToAdd.AsArray());

                for (int j = 0; j < addedTypesCount; j++)
                {
                    var addedType = new ComponentType { TypeIndex = AddedTypes[j], AccessModeType = ComponentType.AccessMode.ReadWrite };
                    if (!ContainsComponent(AllTypesToCopy, addedType))
                        NewArchetypeComponents.Add(addedType);
                }

                var newArchetype = RenderingWorldEntities.CreateArchetype(NewArchetypeComponents.AsArray()); 
                NewArchetypeComponents.Clear();

                if (!newArchetype.Equals(renderingEntityArchetype))
                    RenderingWorldEntities.SetArchetype(remappedEntity, newArchetype);
            }
        }
    }

    [BurstCompile]
    public struct SynchronizeDataJob : IJobChunk
    {
        [ReadOnly] public DynamicComponentHandles ComponentTypeHandles;
        [ReadOnly] public DynamicComponentHandles BufferTypeHandles;
        public DynamicComponentHandles TargetComponentTypeHandles;
        public DynamicComponentHandles TargetBufferTypeHandles;
        [ReadOnly] public NativeArray<ComponentType> ComponentsToCopyTypes;
        [ReadOnly] public NativeArray<ComponentType> BuffersToCopyTypes;
        [ReadOnly] public EntityTypeHandle EntityTypeHandle;
        [NativeDisableParallelForRestriction] public EntityManager RenderingWorldEntities;
        [ReadOnly] public NativeArray<EntityRemapUtility.EntityRemapInfo> EntityRemap;
        public uint LastSystemVersion;

        
        public unsafe void Execute(in ArchetypeChunk chunk, int unfilteredChunkIndex, bool useEnabledMask, in v128 chunkEnabledMask)
        {
            var chunkEntities = chunk.GetNativeArray(EntityTypeHandle);

            for (var i = 0; i < ComponentTypeHandles.Length; i++)
            {
                var componentTypeHandle = ComponentTypeHandles[i];
                var targetComponentTypeHandle = TargetComponentTypeHandles[i];
                var componentType = ComponentsToCopyTypes[i];
                        
                if (chunk.Has(ref componentTypeHandle) && chunk.DidChange(ref componentTypeHandle, LastSystemVersion))
                {
                    var componentEntityOffsets = TypeManager.GetEntityOffsets(componentType.TypeIndex, out var offsetCount);
                    var componentSize = TypeManager.GetTypeInfo(componentType.TypeIndex).SizeInChunk;
                    var componentData = chunk.GetDynamicComponentDataArrayReinterpret<byte>(ref componentTypeHandle, componentSize);
                     
                    for (int j = 0; j < chunk.Count; j++)
                    {
                        var remappedEntity = RemapUtils.RemapEntity(ref EntityRemap, chunkEntities[j]);
                        var storageInfo = RenderingWorldEntities.GetStorageInfo(remappedEntity);
                        var remappedEntityTargetComponent = storageInfo.Chunk.GetDynamicComponentDataArrayReinterpret<byte>(ref targetComponentTypeHandle, componentSize);
                        NativeArray<byte>.Copy(componentData, j * componentSize, remappedEntityTargetComponent, storageInfo.IndexInChunk * componentSize, componentSize);
                        
                        var targetComponentStartPtr = (byte*)remappedEntityTargetComponent.GetSubArray(storageInfo.IndexInChunk * componentSize, componentSize).GetUnsafePtr();

                        for (int k = 0; k < offsetCount; k++)
                        {
                            var entity = (Entity*)(targetComponentStartPtr + componentEntityOffsets[k].Offset);
                            *entity = RemapUtils.RemapEntity(ref EntityRemap, *entity);;
                        }
                    }
                }
            }

            for (int i = 0; i < BufferTypeHandles.Length; i++)
            {
                var bufferTypeHandle = BufferTypeHandles[i];
                var targetBufferTypeHandle = TargetBufferTypeHandles[i];
                var bufferType = BuffersToCopyTypes[i];
                        
                if (chunk.Has(ref bufferTypeHandle) && chunk.DidChange(ref bufferTypeHandle, LastSystemVersion))
                {
                    var componentEntityOffsets = TypeManager.GetEntityOffsets(bufferType.TypeIndex, out var offsetCount);
                    var bufferTypeInfo = TypeManager.GetTypeInfo(bufferType.TypeIndex);
                    var bufferAccessor = chunk.GetUntypedBufferAccessor(ref bufferTypeHandle);
                     
                    for (int j = 0; j < chunk.Count; j++)
                    {
                        var remappedEntity = RemapUtils.RemapEntity(ref EntityRemap, chunkEntities[j]);
                        var storageInfo = RenderingWorldEntities.GetStorageInfo(remappedEntity);
                        var bufferToCopy = (byte*)bufferAccessor.GetUnsafeReadOnlyPtrAndLength(j, out var toCopyLength);
                        
                        var remappedEntityTargetBuffer = storageInfo.Chunk.GetUntypedBufferAccessor(ref targetBufferTypeHandle);
                        remappedEntityTargetBuffer.ResizeUninitialized(storageInfo.IndexInChunk, toCopyLength);
                        var remappedEntityBuffer = (byte*)remappedEntityTargetBuffer.GetUnsafePtr(storageInfo.IndexInChunk);

                        for (int k = 0; k < toCopyLength * bufferTypeInfo.ElementSize; k++) 
                            remappedEntityBuffer[k] = bufferToCopy[k];

                        for (int k = 0; k < toCopyLength; k++)
                        {
                            var startPtr = remappedEntityBuffer + k * bufferTypeInfo.ElementSize;
                            for (int l = 0; l < offsetCount; l++)
                            {
                                var entity = (Entity*)(startPtr + componentEntityOffsets[l].Offset);
                                *entity = RemapUtils.RemapEntity(ref EntityRemap, *entity);
                            }
                        }
                    }
                }
            }
        }
    }

    public struct DynamicComponentHandles
    {
        public const int MaxHandles = 8;
        public DynamicComponentTypeHandle handle1;
        public DynamicComponentTypeHandle handle2;
        public DynamicComponentTypeHandle handle3;
        public DynamicComponentTypeHandle handle4;
        public DynamicComponentTypeHandle handle5;
        public DynamicComponentTypeHandle handle6;
        public DynamicComponentTypeHandle handle7;
        public DynamicComponentTypeHandle handle8;
        public int Length;

        public DynamicComponentHandles(int length)
        {
            Length = length;
            handle1 = default;
            handle2 = default;
            handle3 = default;
            handle4 = default;
            handle5 = default;
            handle6 = default;
            handle7 = default;
            handle8 = default;
        }
        
        public unsafe DynamicComponentTypeHandle this[int index]
        {
            readonly get
            {
                CheckIndexInRange(index, MaxHandles);
                fixed (DynamicComponentTypeHandle* start = &handle1)
                    return *(start + index);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                CheckIndexInRange(index, MaxHandles);
                fixed (DynamicComponentTypeHandle* start = &handle1)
                    *(start + index) = value;
            }
        }

        [Conditional("ENABLE_UNITY_COLLECTIONS_CHECKS"), Conditional("UNITY_DOTS_DEBUG")]
        internal static void CheckIndexInRange(int index, int length)
        {
            // This checks both < 0 and >= Length with one comparison
            if ((uint)index >= (uint)length)
                throw new IndexOutOfRangeException($"Index {index} is out of range in container of '{length}' Length.");
        }
    }
    
    public struct SimulationComponent : IComponentData
    {
        public float3 SomeValue;
    }
    
    public struct SimulationComponent2 : IComponentData
    {
        public float4x4 SomeValue;
    }
    
    public struct SimulationComponent3 : IComponentData
    {
        public float4x4 SomeValue;
    }
    
    public struct SimulationComponent4 : IComponentData
    {
        public float SomeValue;
    }
    
    public struct SimulationComponent5 : IComponentData
    {
        public float SomeValue;
    }
    
    public struct SimulationBuffer1 : IBufferElementData
    {
        public float SomeValue;
        public Entity Entity1;
        public Entity Entity2;
    }
    
    public struct RenderingComponent1 : IComponentData
    {
        public float3 SomeValue;
    }
    
    public struct EntityTransactionSingleton : IComponentData
    {
        public EntityManager EntityManager;
    }

    public partial class MyRenderingSystem : SystemBase
    {
        private EntityQuery _simulationBufferQuery;
        
        protected override void OnCreate()
        {
            _simulationBufferQuery = GetEntityQuery(typeof(SimulationBuffer1));
        }

        protected override void OnUpdate()
        {
            EntityManager.AddComponent<RenderingComponent1>(_simulationBufferQuery);
        }
    }
    
    public class CustomBootstrap : ICustomBootstrap
    {
        public virtual bool Initialize(string defaultWorldName)
        {
            var renderingWorld = new World("Rendering World");
            DefaultWorldInitialization.AddSystemsToRootLevelSystemGroups(renderingWorld, new List<Type> { typeof(MyRenderingSystem) });
            ScriptBehaviourUpdateOrder.AppendWorldToCurrentPlayerLoop(renderingWorld);
            
            var customSystems = new List<Type>()
            {
                typeof(GameSimulationGroup),
                typeof(BeginTransaction),
                typeof(CreateEntities),
                typeof(ProcessEntitiesISystem),
                typeof(DeleteEntities),
                typeof(EndTransactionGroup),
                typeof(EndTransaction),
            };
            
            var simulationWorld = new World("Simulation World");
            DefaultWorldInitialization.AddSystemsToRootLevelSystemGroups(simulationWorld, customSystems);
            ScriptBehaviourUpdateOrder.AppendWorldToCurrentPlayerLoop(simulationWorld);

            var simulationGroup = simulationWorld.GetExistingSystemManaged<GameSimulationGroup>();
            simulationGroup.RateManager = new TransactionRateManager();
            simulationGroup.SetRateManagerCreateAllocator(simulationGroup.RateManager);

            var world = new World(defaultWorldName);
            World.DefaultGameObjectInjectionWorld = world;
            DefaultWorldInitialization.AddSystemsToRootLevelSystemGroups(world, new List<Type>());
            ScriptBehaviourUpdateOrder.AppendWorldToCurrentPlayerLoop(world);

            return true;
        }
    }

    public unsafe class TransactionRateManager : IRateManager
    {
        private int _lastFrameUpdate;
        DoubleRewindableAllocators* _oldGroupAllocators = null;
        
        public bool ShouldGroupUpdate(ComponentSystemGroup group)
        {
            if (_lastFrameUpdate != Time.frameCount)
            {
                _lastFrameUpdate = Time.frameCount;
                _oldGroupAllocators = group.World.CurrentGroupAllocators;
                group.World.SetGroupAllocator(group.RateGroupAllocators);
                return group.EntityManager.CanBeginExclusiveEntityTransaction();
            }

            group.World.RestoreGroupAllocator(_oldGroupAllocators);
            return false;
        }

        public float Timestep { get; set; }
    }

    public static class RemapUtils
    {
        // Safe version of EntityRemapUtility.RemapEntity
        public static Entity RemapEntity(ref NativeArray<EntityRemapUtility.EntityRemapInfo> remapping, Entity source)
        {
            if (source.Version == remapping[source.Index].SourceVersion)
                return remapping[source.Index].Target;
            // When moving whole worlds, we do not allow any references that aren't in the new world
            // to avoid any kind of accidental references
            return Entity.Null;
        }
    }
}