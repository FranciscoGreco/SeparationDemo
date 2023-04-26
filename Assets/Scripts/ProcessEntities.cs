using System;
using System.Collections.Generic;
using System.Diagnostics;
using Unity.Assertions;
using Unity.Collections;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;

namespace DefaultNamespace
{
    [UpdateInGroup(typeof(SimulationSystemGroup))]
    [DisableAutoCreation]
    public partial class MySimulationGroup : ComponentSystemGroup
    {
        protected override void OnUpdate()
        {
            if (EntityManager.CanBeginExclusiveEntityTransaction())
                base.OnUpdate();
        }
    }
    
    [UpdateInGroup(typeof(MySimulationGroup), OrderFirst = true)]
    [DisableAutoCreation]
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

    [UpdateInGroup(typeof(MySimulationGroup))]
    [DisableAutoCreation]
    public partial class CreateEntities : SystemBase
    {
        private EntityArchetype _newArchetype;
        
        protected override void OnCreate()
        {
            _newArchetype = EntityManager.CreateArchetype(typeof(MyComponent));
        } 

        protected override void OnUpdate()
        {
            var transactionSingleton = SystemAPI.GetSingletonRW<EntityTransactionSingleton>();

            Dependency = new CreateEntitiesInJob
            {
                EntityManager = transactionSingleton.ValueRW.EntityManager,
                Archetype = _newArchetype
            }.Schedule(Dependency);

            var manager = EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, Dependency);
        }
    }

    public struct CreateEntitiesInJob : IJob
    {
        public EntityArchetype Archetype;
        public EntityManager EntityManager;
        
        public void Execute()
        {
            EntityManager.CreateEntity(Archetype, 10);
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
    [UpdateInGroup(typeof(MySimulationGroup))]
    [DisableAutoCreation]
    public partial class ProcessEntities : SystemBase
    {
        private EntityArchetype _newArchetype;
    
        protected override void OnCreate()
        {
            _newArchetype = EntityManager.CreateArchetype(typeof(MyComponent));
        }
    
        protected override void OnUpdate()
        {
            Entities.WithName("DummyWork").ForEach((ref MyComponent dummyComponent) =>
            {
                dummyComponent.SomeValue += 400;
            }).Schedule();
            
            Entities.WithName("DummyWork2").ForEach((ref MyComponent dummyComponent) =>
            {
                dummyComponent.SomeValue += 400;
            }).Schedule();
    
            Dependency = new StallJob().Schedule(Dependency);
            
            Dependency = new CreateEntitiesInJob
            {
                EntityManager = SystemAPI.GetSingletonRW<EntityTransactionSingleton>().ValueRW.EntityManager,
                Archetype = _newArchetype
            }.Schedule(Dependency);
    
            var manager = EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, Dependency);
        }
    }
    
    [UpdateAfter(typeof(CreateEntities))]
    [UpdateInGroup(typeof(MySimulationGroup))]
    [DisableAutoCreation]
    public partial struct ProcessEntitiesISystem : ISystem
    {
        private EntityArchetype _newArchetype;
    
        public void OnCreate(ref SystemState state)
        {
            _newArchetype = state.EntityManager.CreateArchetype(typeof(MyComponent));
        }
    
        public void OnUpdate(ref SystemState state)
        {
            state.Dependency = new CreateEntitiesInJob
            {
                EntityManager = SystemAPI.GetSingletonRW<EntityTransactionSingleton>().ValueRW.EntityManager,
                Archetype = _newArchetype
            }.Schedule(state.Dependency);

            new DummyProcessJob().ScheduleParallel();
    
            var manager = state.EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, state.Dependency);
        }
    }

    public partial struct DummyProcessJob : IJobEntity
    {
        public void Execute(ref MyComponent dummyComponent)
        {
            dummyComponent.SomeValue -= 100;
        }
    }
    
    [UpdateAfter(typeof(ProcessEntities))]
    [UpdateAfter(typeof(ProcessEntitiesISystem))]
    [UpdateInGroup(typeof(MySimulationGroup))]
    [DisableAutoCreation]
    public partial class DeleteEntities : SystemBase
    {
        protected override void OnUpdate()
        {
            Dependency = new DeleteEntitiesInJob
            {
                EntityManager = SystemAPI.GetSingletonRW<EntityTransactionSingleton>().ValueRW.EntityManager,
            }.Schedule(Dependency);
    
            var manager = EntityManager;
            manager.ExclusiveEntityTransactionDependency = JobHandle.CombineDependencies(manager.ExclusiveEntityTransactionDependency, Dependency);
        }
    }
    
    public struct DeleteEntitiesInJob : IJob
    {
        public EntityManager EntityManager;
        
        public void Execute()
        {
            if (EntityManager.UniversalQuery.CalculateEntityCount() > 10000)
                EntityManager.DestroyEntity(EntityManager.UniversalQuery);
        }
    }

    [UpdateInGroup(typeof(SimulationSystemGroup), OrderLast = true)]
    [DisableAutoCreation]
    public partial class EndTransaction : SystemBase
    {
        private EntityManager renderingWorldEntities;
        private NativeList<int> previousState;
        private NativeList<EntityRemapUtility.EntityRemapInfo> entityRemap;

        protected override void OnCreate()
        {
            foreach (var world in World.All)
            {
                if (world.Name == "Default World")
                    renderingWorldEntities = world.EntityManager;
            }

            previousState = new NativeList<int>(Allocator.Persistent);
            entityRemap = new NativeList<EntityRemapUtility.EntityRemapInfo>(Allocator.Persistent);
        }

        protected override void OnUpdate()
        {
            EntityManager.ExclusiveEntityTransactionDependency.Complete();

            if (EntityManager.ExclusiveEntityTransactionDependency.IsCompleted)
            {
                EntityManager.EndExclusiveEntityTransaction();
                
                using var createdEntities = new NativeList<Entity>(Allocator.TempJob);
                using var destroyedEntities = new NativeList<Entity>(Allocator.TempJob);

                EntityManager.GetCreatedAndDestroyedEntities(previousState, createdEntities, destroyedEntities);
                
                // For now exclude all System singleton entities
                for (int i = createdEntities.Length - 1; i >= 0; i--)
                {
                    if (!EntityManager.UniversalQuery.MatchesIgnoreFilter(createdEntities[i]))
                        createdEntities.RemoveAtSwapBack(i);
                }

                foreach (var createdEntity in createdEntities)
                {
                    if (createdEntity.Index >= entityRemap.Length)
                        entityRemap.Resize(createdEntity.Index + 1, NativeArrayOptions.ClearMemory);

                    var remapArray = entityRemap.AsArray();
                    EntityRemapUtility.AddEntityRemapping(ref remapArray, createdEntity, renderingWorldEntities.CreateEntity());
                }
                
                var destroyedEntitiesRemap = new NativeList<Entity>(destroyedEntities.Length, Allocator.Temp);
                var entityRemapArray = entityRemap.AsArray();
                foreach (var entity in destroyedEntities)
                    destroyedEntitiesRemap.Add(EntityRemapUtility.RemapEntity(ref entityRemapArray, entity));
                renderingWorldEntities.DestroyEntity(destroyedEntitiesRemap);

                foreach (var chunk in EntityManager.GetAllChunks())
                {
                    //TODO: Add/Remove components in rendering world, remap entities and copy values from simulation world
                }

                Assert.AreEqual(renderingWorldEntities.UniversalQuery.CalculateEntityCount() - 1, EntityManager.UniversalQuery.CalculateEntityCount());
            }
        }
    }
    
    public struct MyComponent : IComponentData
    {
        public float3 SomeValue;
    }

    public struct EntityTransactionSingleton : IComponentData
    {
        public EntityManager EntityManager;
    }
    
    public class CustomBootstrap : ICustomBootstrap
    {
        public virtual bool Initialize(string defaultWorldName)
        {
            // The default world must be created before generating the system list in order to have a valid TypeManager instance.
            // The TypeManage is initialised the first time we create a world.
            var world = new World(defaultWorldName);
            World.DefaultGameObjectInjectionWorld = world;
            var systems = DefaultWorldInitialization.GetAllSystems(WorldSystemFilterFlags.Default);

            DefaultWorldInitialization.AddSystemsToRootLevelSystemGroups(world, systems);
            ScriptBehaviourUpdateOrder.AppendWorldToCurrentPlayerLoop(world);

            var customSystems = new List<Type>()
            {
                typeof(MySimulationGroup),
                typeof(BeginTransaction),
                typeof(CreateEntities),
                typeof(ProcessEntities),
                typeof(ProcessEntitiesISystem),
                typeof(DeleteEntities),
                typeof(EndTransaction)
            };
            
            var serverWorld = new World("SimulationWorld");
            DefaultWorldInitialization.AddSystemsToRootLevelSystemGroups(serverWorld, customSystems);
            ScriptBehaviourUpdateOrder.AppendWorldToCurrentPlayerLoop(serverWorld);

            return true;
        }
    }
}