# !/usr/bin/env python3

# NOTE: this requires jaxrl_m to be installed: 
#       https://github.com/rail-berkeley/jaxrl_minimal

import jax
import chex
import gym
import numpy as np
from typing import Optional

from jaxrl_m.agents.continuous.sac import SACAgent
from jaxrl_m.common.wandb import WandBLogger

from edgeml.trainer import TrainerConfig
from edgeml.data.data_store import DataStoreBase
from edgeml.data.replay_buffer import EfficientReplayBuffer, DataShape
from edgeml.data.sampler import LatestSampler, SequenceSampler

from jaxrl_m.common.wandb import WandBLogger
from jaxrl_m.agents.continuous.sac import SACAgent
from jaxrl_m.data.replay_buffer import ReplayBuffer
from edgeml.trainer import TrainerConfig

from threading import Lock

from jax import nn

##############################################################################


class ReplayBufferDataStore(ReplayBuffer, DataStoreBase):
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        capacity: int,
    ):
        ReplayBuffer.__init__(self, observation_space, action_space, capacity)
        DataStoreBase.__init__(self, capacity)
        self._lock = Lock()

    # ensure thread safety
    def insert(self, *args, **kwargs):
        with self._lock:
            super(ReplayBufferDataStore, self).insert(*args, **kwargs)

    # ensure thread safety
    def sample(self, *args, **kwargs):
        with self._lock:
            return super(ReplayBufferDataStore, self).sample(*args, **kwargs)

    # NOTE: method for DataStoreBase
    def latest_data_id(self):
        return self._insert_index

    # NOTE: method for DataStoreBase
    def get_latest_data(self, from_id: int):
        raise NotImplementedError  # TODO


##############################################################################


def make_agent(sample_obs, sample_action):
    return SACAgent.create_states(
        jax.random.PRNGKey(0),
        sample_obs,
        sample_action,
        policy_kwargs={
            "tanh_squash_distribution": True,
            "std_parameterization": "softplus",
        },
        critic_network_kwargs={
            "activations": nn.tanh,
            "use_layer_norm": True,
            "hidden_dims": [256, 256],
        },
        policy_network_kwargs={
            "activations": nn.tanh,
            "use_layer_norm": True,
            "hidden_dims": [256, 256],
        },
        temperature_init=1e-2,
        discount=0.99,
        backup_entropy=True,
        critic_ensemble_size=10,
        critic_subsample_size=2,
    )


def make_trainer_config():
    return TrainerConfig(
        port_number=5488,
        broadcast_port=5489,
        request_types=["send-stats"]
    )


def make_wandb_logger(
    project: str = "edgeml",
    description: str = "jaxrl_m",
):
    wandb_config = WandBLogger.get_default_config()
    wandb_config.update(
        {
            "project": project,
            "exp_descriptor": description,
        }
    )
    wandb_logger = WandBLogger(
        wandb_config=wandb_config,
        variant={},
    )
    return wandb_logger


def make_efficient_replay_buffer(
    observation_space: gym.Space,
    action_space: gym.Space,
    capacity: int,
    device: Optional[jax.Device] = None,
):
    replay_buffer = EfficientReplayBuffer(
        capacity=capacity,
        data_shapes=[
            DataShape("observations", observation_space.shape, np.float32),
            DataShape("next_observations", observation_space.shape, np.float32),
            DataShape("actions", action_space.shape, np.float32),
            DataShape("rewards", (), np.float32),
            DataShape("masks", (), np.float32),
            DataShape("end_of_trajectory", (), dtype="bool"),
        ],
        min_trajectory_length=2,
        device=device,
    )

    @jax.jit
    def transform_rl_data(batch, mask):
        batch_size = jax.tree_util.tree_flatten(batch)[0][0].shape[0]
        chex.assert_tree_shape_prefix(batch["observations"], (batch_size, 2))
        chex.assert_tree_shape_prefix(mask["observations"], (batch_size, 2))
        return {
            **batch,
            "observations": batch["observations"][:, 0],
            "next_observations": batch["observations"][:, 1],
        }, {
            **mask,
            "observations": mask["observations"][:, 0],
            "next_observations": mask["observations"][:, 1],
        }

    replay_buffer.register_sample_config(
        "training",
        samplers={
            "observations": SequenceSampler(
                squeeze=False, begin=0, end=2, source="observations"
            ),
            "actions": LatestSampler(),
            "rewards": LatestSampler(),
            "masks": LatestSampler(),
            "next_observations": LatestSampler(),
            "end_of_trajectory": LatestSampler(),
        },
        transform=transform_rl_data,
        sample_range=(0, 2),
    )
    return replay_buffer
